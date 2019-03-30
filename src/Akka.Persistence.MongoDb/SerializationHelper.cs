// -----------------------------------------------------------------------
// <copyright file="SerializationHelper.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Akka.Actor;
using Akka.Persistence.Serialization;
using Akka.Serialization;
using MongoDB.Bson;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace Akka.Persistence.Helpers
{
    /// <summary>
    ///     INTERNAL API
    /// </summary>
    internal sealed class SerializationHelper
    {
        private readonly ActorSystem _actorSystem;

        private readonly Type _persistentRepresentation = typeof(IPersistentRepresentation);
        private readonly Type _snapshotType = typeof(Serialization.Snapshot);

        public SerializationHelper(ActorSystem actorSystem)
        {
            _actorSystem = actorSystem;
        }

        private JsonSerializer serializer = JsonSerializer.Create(new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.All });

        //public string PersistentToString(IPersistentRepresentation message)
        //{
        //    /*
        //     * Implementation note: Akka.NET caches the serialization lookups internally here,
        //     * so there's no need to do it again.
        //     */
        //    var sw = new StringWriter();
        //    var w = new JsonTextWriter(sw);
        //    serializer.Serialize(w, message);
        //    w.Flush();
        //    return w.ToString();
        //    //var serializer = _actorSystem.Serialization.FindSerializerForType(_persistentRepresentation);
        //    //return serializer.ToBinary(message);
        //}

        //public IPersistentRepresentation PersistentFromString(string bytes)
        //{
        //    /*
        //     * Implementation note: Akka.NET caches the serialization lookups internally here,
        //     * so there's no need to do it again.
        //     */

        //    //var serializer = _actorSystem.Serialization.FindSerializerForType(_persistentRepresentation);
        //    //var msg = serializer.FromBinary<IPersistentRepresentation>(bytes);
        //    //return msg;
        //    var r = new JsonTextReader(new StringReader(bytes));
        //    return serializer.Deserialize<IPersistentRepresentation>(r);
        //}

        //public IPersistentRepresentation PersistentFromBytesWithManifest(byte[] bytes, string manifest)
        //{
        //    /*
        //     * Implementation note: Akka.NET caches the serialization lookups internally here,
        //     * so there's no need to do it again.
        //     */

        //    var serializer = _actorSystem.Serialization.FindSerializerForType(_persistentRepresentation);
        //    if (serializer is SerializerWithStringManifest manifestSerializer)
        //        return (IPersistentRepresentation) manifestSerializer.FromBinary(bytes, manifest);

        //    return serializer.FromBinary<IPersistentRepresentation>(bytes);
        //}

        //public string SnapshotToString(Serialization.Snapshot snapshot)
        //{
        //    //var serializer = _actorSystem.Serialization.FindSerializerForType(_snapshotType);
        //    //return serializer.ToBinary(snapshot);
        //    var sw = new StringWriter();
        //    var w = new JsonTextWriter(sw);
        //    serializer.Serialize(w, snapshot);
        //    w.Flush();
        //    return w.ToString();
        //}

        //public Serialization.Snapshot SnapshotFromString(string bytes)
        //{
        //    //var serializer = _actorSystem.Serialization.FindSerializerForType(_snapshotType);
        //    //return serializer.FromBinary<Serialization.Snapshot>(bytes);
        //    var r = new JsonTextReader(new StringReader(bytes));
        //    return serializer.Deserialize<Serialization.Snapshot>(r);
        //}

        internal RawBsonDocument PersistentToBson(IPersistentRepresentation message)
        {
            using (MemoryStream ms = new MemoryStream())
            using (BsonDataWriter datawriter = new BsonDataWriter(ms))
            {
                serializer.Serialize(datawriter, message);
                return new RawBsonDocument(ms.ToArray());
            }

        }

        internal IPersistentRepresentation PersistentFromBson(RawBsonDocument payload)
        {
            var data = new byte[payload.Slice.Length];
            payload.Slice.GetBytes(0, data, 0, data.Length);

            using (MemoryStream ms = new MemoryStream(data))
            using (BsonDataReader reader = new BsonDataReader(ms))
            {
                return serializer.Deserialize<IPersistentRepresentation>(reader);
            }
        }

        internal RawBsonDocument SnapshotToBson(Serialization.Snapshot snapshot)
        {
            using (MemoryStream ms = new MemoryStream())
            using (BsonDataWriter datawriter = new BsonDataWriter(ms))
            {
                serializer.Serialize(datawriter, snapshot.Data);
                return new RawBsonDocument(ms.ToArray());
            }
        }

        internal Serialization.Snapshot SnapshotFromBson(RawBsonDocument snapshot)
        {
            var data = new byte[snapshot.Slice.Length];
            snapshot.Slice.GetBytes(0, data, 0, data.Length);

            using (MemoryStream ms = new MemoryStream(data))
            using (BsonDataReader reader = new BsonDataReader(ms))
            {
                var obj = serializer.Deserialize(reader);
                return new Serialization.Snapshot(obj);
            }
        }
    }
}