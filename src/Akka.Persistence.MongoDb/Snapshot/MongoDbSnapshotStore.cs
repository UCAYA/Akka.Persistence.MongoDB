//-----------------------------------------------------------------------
// <copyright file="MongoDbSnapshotStore.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Akka.Persistence.Helpers;
using Akka.Persistence.Snapshot;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace Akka.Persistence.MongoDb.Snapshot
{
    /// <summary>
    /// A SnapshotStore implementation for writing snapshots to MongoDB.
    /// </summary>
    public class MongoDbSnapshotStore : SnapshotStore
    {
        private readonly MongoDbSnapshotSettings _settings;
        private Lazy<IMongoCollection<SnapshotEntry>> _snapshotCollection;
        private readonly SerializationHelper _serialization;

        public MongoDbSnapshotStore()
        {
            _settings = MongoDbPersistence.Get(Context.System).SnapshotStoreSettings;
            _serialization = new SerializationHelper(Context.System);
        }

        protected override void PreStart()
        {
            base.PreStart();

            _snapshotCollection = new Lazy<IMongoCollection<SnapshotEntry>>(() =>
            {
                var connectionString = new MongoUrl(_settings.ConnectionString);
                var client = new MongoClient(connectionString);

                var snapshot = client.GetDatabase(connectionString.DatabaseName);

                var collection = snapshot.GetCollection<SnapshotEntry>(_settings.Collection);
                if (_settings.AutoInitialize)
                {
                    collection.Indexes.CreateOneAsync(
                        Builders<SnapshotEntry>.IndexKeys
                        .Ascending(entry => entry.PersistenceId)
                        .Descending(entry => entry.SequenceNr))
                        .Wait();
                }

                return collection;
            });
        }

        protected override Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var filter = CreateRangeFilter(persistenceId, criteria);

            return
                _snapshotCollection.Value
                    .Find(filter)
                    .SortByDescending(x => x.SequenceNr)
                    .Limit(1)
                    .Project(x => ToSelectedSnapshot(x))
                    .FirstOrDefaultAsync();
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var snapshotEntry = ToSnapshotEntry(metadata, snapshot);

            await _snapshotCollection.Value.ReplaceOneAsync(
                CreateSnapshotIdFilter(snapshotEntry.Id),
                snapshotEntry,
                new UpdateOptions { IsUpsert = true });
        }

        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            var builder = Builders<SnapshotEntry>.Filter;
            var filter = builder.Eq(x => x.PersistenceId, metadata.PersistenceId);

            if (metadata.SequenceNr > 0 && metadata.SequenceNr < long.MaxValue)
                filter &= builder.Eq(x => x.SequenceNr, metadata.SequenceNr);

            if (metadata.Timestamp != DateTime.MinValue && metadata.Timestamp != DateTime.MaxValue)
                filter &= builder.Eq(x => x.Timestamp, metadata.Timestamp.Ticks);

            return _snapshotCollection.Value.FindOneAndDeleteAsync(filter);
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var filter = CreateRangeFilter(persistenceId, criteria);

            return _snapshotCollection.Value.DeleteManyAsync(filter);
        }

        private static FilterDefinition<SnapshotEntry> CreateSnapshotIdFilter(string snapshotId)
        {
            var builder = Builders<SnapshotEntry>.Filter;

            var filter = builder.Eq(x => x.Id, snapshotId);

            return filter;
        }

        private static FilterDefinition<SnapshotEntry> CreateRangeFilter(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var builder = Builders<SnapshotEntry>.Filter;
            var filter = builder.Eq(x => x.PersistenceId, persistenceId);

            if (criteria.MaxSequenceNr > 0 && criteria.MaxSequenceNr < long.MaxValue)
                filter &= builder.Lte(x => x.SequenceNr, criteria.MaxSequenceNr);

            if (criteria.MaxTimeStamp != DateTime.MinValue && criteria.MaxTimeStamp != DateTime.MaxValue)
                filter &= builder.Lte(x => x.Timestamp, criteria.MaxTimeStamp.Ticks);

            return filter;
        }


        //private JournalEntry ToJournalEntry(IPersistentRepresentation message)
        //{
            

        //    return new JournalEntry
        //    {
        //        Id = message.PersistenceId + "_" + message.SequenceNr,
        //        IsDeleted = message.IsDeleted,
        //        Payload = new RawBsonDocument(ms.ToArray()),
        //        PersistenceId = message.PersistenceId,
        //        SequenceNr = message.SequenceNr,
        //        Manifest = message.Manifest
        //    };
        //}

        //private Persistent ToPersistenceRepresentation(JournalEntry entry, IActorRef sender)
        //{
        //    var doc = (RawBsonDocument)entry.Payload;
        //    var bytes = new byte[doc.Slice.Length];
        //    doc.Slice.GetBytes(0, bytes, 0, doc.Slice.Length);
        //    object payload = null;
        //    using (var reader = new BsonDataReader(new MemoryStream(bytes)))
        //    {
        //        JsonSerializer serializer;

        //        if (JsonSerializerSettings != null)
        //            serializer = JsonSerializer.Create(JsonSerializerSettings);
        //        else
        //            serializer = JsonSerializer.CreateDefault();
        //        payload = serializer.Deserialize(reader);
        //    }

        //    return new Persistent(payload, entry.SequenceNr, entry.PersistenceId, entry.Manifest, entry.IsDeleted, sender);
        //}


        private SnapshotEntry ToSnapshotEntry(SnapshotMetadata metadata, object snapshot)
        {
            var snapshotData = _serialization.SnapshotToBson(new Serialization.Snapshot(snapshot));

            return new SnapshotEntry
            {
                Id = metadata.PersistenceId + "_" + metadata.SequenceNr,
                PersistenceId = metadata.PersistenceId,
                SequenceNr = metadata.SequenceNr,
                Snapshot = snapshotData,
                Timestamp = metadata.Timestamp.Ticks
            };
        }

        private SelectedSnapshot ToSelectedSnapshot(SnapshotEntry entry)
        {
            var snapshot = _serialization.SnapshotFromBson(entry.Snapshot);
            return new SelectedSnapshot(new SnapshotMetadata(entry.PersistenceId, entry.SequenceNr, new DateTime(entry.Timestamp)), snapshot.Data);
        }
    }
}
