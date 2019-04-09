using System;
using System.Collections.Generic;
using System.Net;
using Flurl;
using Flurl.Http;
using System.Reactive.Linq;
using System.Collections;
using System.Linq;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace es_cluster_scavenge
{
    class EventStoreNode
    {
        private readonly string _user;
        private readonly string _pass;

        public EventStoreNode(IPEndPoint nodeHttpEndpoint, string user, string pass)
        {
            NodeHttpEndpoint = nodeHttpEndpoint;
            _user = user;
            _pass = pass;
        }

        public IPEndPoint NodeHttpEndpoint { get; }

        public async Task RunScavengeAsync(TimeSpan retentionDuration, int threads, CancellationToken ct)
        {
            if (!ct.CanBeCanceled)
                throw new ArgumentException(nameof(ct), "You must pass in a cancelable token");

            // Run some strict checks here 
            //  - is a scavenge already happening?

            var chunk = await GetRecommendedScavengeChunk(retentionDuration);

            await RunScavengeAsync(chunk, threads, ct);
        }

        public async Task RunScavengeAsync(int startFromChunk, int threads, CancellationToken ct)
        {
            if (!ct.CanBeCanceled)
                throw new ArgumentException(nameof(ct), "You must pass in a cancelable token");

            if (ct.IsCancellationRequested)
                return;

            // Run some strict checks here 
            //  - is a scavenge already happening?

            var scavengeInstance = await StartScavenge(startFromChunk, threads);

            await ct.WhenCanceled();

            await StopScavenge(scavengeInstance);
        }

        public async Task StopAllScavenges()
        {
            var myScavenges = ReadMyScavengesBackwards()
                .Where(si => !si.Completed)
                .ToEnumerable();

            var stopAll = myScavenges
                .Select(async si =>
                {
                    try
                    {
                        await StopScavenge(si);
                    }
                    catch
                    {
                        // Ignore any errors stopping scavenges as it's just best effort
                    }
                });

            await Task.WhenAll(stopAll);
        }

        public async Task<ScavengeInstance> StartScavenge(int startFromChunk, int threads)
        {
            var uri = $"http://{NodeHttpEndpoint}/admin/scavenge";

            var response = await uri
                .SetQueryParam("startFromChunk", startFromChunk)
                .SetQueryParam("threads", threads)
                .WithHeader("Accept", "application/json")
                .WithBasicAuth(_user, _pass)
                .PostStringAsync(string.Empty)
                .ReceiveJson();

            return new ScavengeInstance { Id = response.scavengeId };
        }

        public async Task StopScavenge(ScavengeInstance scavengeInstance)
        {
            var uri = $"http://{NodeHttpEndpoint}/admin/scavenge/{scavengeInstance.Id}";

            await uri
                .WithBasicAuth(_user, _pass)
                .DeleteAsync();
        }
        public async Task<int> GetRecommendedScavengeChunk(TimeSpan retentionDuration)
        {
            var lastScavenge = await FindLastScavenge();

            if (lastScavenge == null)
                return 0;

            var lastChunk = await FindLastScavengedChunk(lastScavenge);

            var retentionPassedDate = lastChunk.Timestamp - retentionDuration;

            var recommendedChunk = await FindChunkAtDate(lastChunk.ChunkNumber, retentionPassedDate);

            return recommendedChunk.Chunk;
        }


        /// <summary>
        /// Search backwards to find the newest chunk that was created before the input date...
        /// </summary>
        /// <param name="startFromChunk"></param>
        /// <param name="date"></param>
        /// <returns></returns>
        public async Task<ChunkMetadata> FindChunkAtDate(int startFromChunk, DateTimeOffset date)
        {
            while (startFromChunk > 0)
            {
                var chunkMetadata = await GetChunkMetadata(startFromChunk);

                // if we've gone past the date
                if (chunkMetadata.CreatedAt <= date)
                    return chunkMetadata;

                startFromChunk--;
            }

            // otherwise just return chunk 0
            return await GetChunkMetadata(0);
        }


        /// <summary>
        /// TODO - handle multiple threads
        /// One thread could end up significantly behind and you'd end up misreporting the last chunk
        /// Probably good enough for now
        /// </summary>
        /// <param name="scavengeInstance"></param>
        /// <returns></returns>
        public async Task<ScavengeChunkProgress> FindLastScavengedChunk(ScavengeInstance scavengeInstance)
        {
            return await ReadStreamBackward($"$scavenges-{scavengeInstance.Id}")
                .Where(e => e.EventType == "$scavengeChunksCompleted")
                .Select(e => new ScavengeChunkProgress { Scavenge = scavengeInstance, ChunkNumber = e.JsonData.chunkStartNumber, Timestamp = e.Timestamp })
                .FirstAsync();
        }

        private IObservable<ScavengeInstance> ReadMyScavengesBackwards()
        {
            return ReadStreamBackward("$scavenges")
                // TODO - need a better way of identifying each node by name - see above
                //.Where(e => e.JsonData.nodeEndpoint == NodeHttpEndpoint.ToString())
                .Select(e => new ScavengeInstance
                {
                    Id = e.JsonData.scavengeId,
                    Completed = (e.EventType == "$scavengeCompleted"),
                    Result = e.JsonData.result
                })
                .Distinct(e => e.Id);
        }

        public async Task<ScavengeInstance> FindLastScavenge()
        {
            return await ReadMyScavengesBackwards()
                .FirstOrDefaultAsync();
        }


        public async Task<ChunkMetadata> GetChunkMetadata(int chunkNumber)
        {
            const int chunkSize = 256 * 1024 * 1024; // 256MB

            var chunkStartPosition = chunkNumber * chunkSize;

            var positionHex = chunkStartPosition.ToString("X");

            var uri = $"http://{NodeHttpEndpoint}/streams/$all/{positionHex}{positionHex}/forward/1";

            var response = await uri
                    .WithHeader("Accept", "application/json")
                    .WithBasicAuth(_user, _pass)
                    .GetJsonAsync();

            DateTimeOffset chunkCreated = (DateTimeOffset)response.updated;

            return new ChunkMetadata { Chunk = chunkNumber, CreatedAt = chunkCreated };
        }

        public IObservable<Entry> ReadStreamBackward(string stream)
        {
            return Observable.Create<Entry>(async (obs, ct) =>
            {
                var url = $"http://{NodeHttpEndpoint}/streams/{stream}";

                do
                {
                    var response = await url
                        .SetQueryParam("embed", "body")
                        .WithHeader("Accept", "application/json")
                        .WithBasicAuth(_user, _pass)
                        .AllowHttpStatus(HttpStatusCode.NotFound)
                        .GetJsonAsync();

                    IEnumerable<dynamic> entries = ((IEnumerable)response?.entries)
                        ?.Cast<dynamic>()
                        ?? Enumerable.Empty<dynamic>();

                    foreach (var e in entries)
                    {
                        obs.OnNext(new Entry
                        {
                            StreamId = e.streamId,
                            EventNumber = e.eventNumber,
                            EventType = e.eventType,
                            JsonData = JObject.Parse(e.data),
                            Timestamp = e.updated,
                            PositionNumber = e.positionEventNumber,
                            PositionStream = e.positionStreamId
                        });
                    }

                    url = ((IEnumerable)response?.links)
                        ?.Cast<dynamic>()
                        ?.FirstOrDefault(l => l.relation == "next")
                        ?.uri;

                } while (url != null);
            });
        }


        // An experimental node id feature added to EventStore, 
        // so if ExtIp are equal e.g. 0.0.0.0:2113 because the port bindings 
        // are handled by an orchestrator e.g. docker
        // I doubt we can use AdvertisedExtIp in scavenge logs as I assume 
        // that is currently used to advertise a common cluster DNS (for load balancing reads)
        // for the HTTP APIs (links, atom feeds etc) so it would suffer from the same issue
        // as 0.0.0.0 not uniquely identifying which node performed the scavenge!
        public async Task<string> GetNodeId()
        {
            var url = $"http://{NodeHttpEndpoint}/info";

            var response = await url
                .WithHeader("Accept", "application/json")
                .GetJsonAsync();

            return response.nodeId ?? NodeHttpEndpoint.ToString();
        }

    }

    internal class ChunkMetadata
    {
        public int Chunk { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    internal class ScavengeChunkProgress
    {
        public ScavengeInstance Scavenge { get; set; }
        public int ChunkNumber { get; set; }
        public DateTimeOffset Timestamp { get; set; }
    }

    internal class ScavengeInstance
    {
        public Guid Id { get; set; }
        public bool Completed { get; set; }
        public string Result { get; set; }
    }

    class Entry
    {
        public string StreamId { get; set; }// = $entry.streamId; 
        public long EventNumber { get; set; }// = $entry.eventNumber; 
        public string EventType { get; set; }// = $entry.eventType; 
        public dynamic JsonData { get; set; }// = (ConvertFrom - Json $entry.data); 
        public DateTimeOffset Timestamp { get; set; }//= $entry.updated; 
        public long PositionNumber { get; set; }// = $entry.positionEventNumber; 
        public string PositionStream { get; set; }// = $entry.positionStreamId
    };

}
