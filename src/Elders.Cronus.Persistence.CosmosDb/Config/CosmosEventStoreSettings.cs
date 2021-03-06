﻿using Elders.Cronus.DomainModeling;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Config;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Serializer;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;

namespace Cronus.Persistence.CosmosDb.Config
{
    public static class CosmosEventStoreExtensions
    {
        public static T UseCosmosEventStore<T>(this T self, Action<CosmosEventStoreSettings> configure) where T : IConsumerSettings<ICommand>
        {
            CosmosEventStoreSettings settings = new CosmosEventStoreSettings(self);
            settings.SetDatabaseName("Elders");
            settings.SetCollectionName("EventStore");
            settings.SetThroughput(2500);
            configure?.Invoke(settings);

            (settings as ISettingsBuilder).Build();
            return self;
        }

        public static T SetDatabaseName<T>(this T self, string databaseName) where T : ICosmosEventStoreSettings
        {
            if (string.IsNullOrWhiteSpace(databaseName)) throw new ArgumentNullException(nameof(databaseName));

            self.DatabaseName = databaseName;

            return self;
        }

        public static T SetThroughput<T>(this T self, int throughput) where T : ICosmosEventStoreSettings
        {
            if (throughput < 2500) throw new ArgumentException("Min is 2500!", nameof(throughput));

            self.Throughput = throughput;

            return self;
        }

        public static T SetCollectionName<T>(this T self, string collectionName) where T : ICosmosEventStoreSettings
        {
            if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentNullException(nameof(collectionName));

            self.CollectionName = collectionName;

            return self;
        }


        public static T SetDocumentclient<T>(this T self, Uri uri, string masterKey) where T : ICosmosEventStoreSettings
        {
            if (uri == null || string.IsNullOrEmpty(masterKey)) throw new ArgumentException("Supply correct information for the DocumentClient Uri and/or MasterKey.");

            self.DocumentClient = new DocumentClient(uri, masterKey);

            return self;
        }

        public static T WithNewStorageIfNotExists<T>(this T self) where T : ICosmosEventStoreSettings
        {
            self.WithNewStorageIfNotExists = true;

            return self;
        }
    }

    public class CosmosEventStoreSettings : SettingsBuilder, ICosmosEventStoreSettings
    {
        public CosmosEventStoreSettings(ISettingsBuilder settingsBuilder) : base(settingsBuilder) { }

        public override void Build()
        {
            var builder = this as ISettingsBuilder;
            ICosmosEventStoreSettings settings = this as ICosmosEventStoreSettings;
            Uri queryUri = UriFactory.CreateDocumentCollectionUri(settings.DatabaseName, settings.CollectionName);

            if (settings.WithNewStorageIfNotExists)
            {
                settings.DocumentClient.CreateDatabaseIfNotExistsAsync(new Database { Id = settings.DatabaseName }).Wait();
                CreateAggregateCollection(settings.DocumentClient, settings.DatabaseName, settings.CollectionName, settings.Throughput);
            }

            var eventStore = new CosmosEventStore(settings.DocumentClient, queryUri, builder.Container.Resolve<ISerializer>());

            builder.Container.RegisterSingleton<IEventStore>(() => eventStore, builder.Name);
        }

        private static void CreateAggregateCollection(DocumentClient client, string databaseId, string collectionId, int throughput)
        {
            var newCollection = new DocumentCollection { Id = collectionId };
            newCollection.PartitionKey.Paths.Add("/i");
            Uri databaseUri = UriFactory.CreateDatabaseUri(databaseId);

            client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, newCollection, new RequestOptions { OfferThroughput = throughput }).Wait();
        }

        string ICosmosEventStoreSettings.DatabaseName { get; set; }

        string ICosmosEventStoreSettings.CollectionName { get; set; }

        int ICosmosEventStoreSettings.Throughput { get; set; }

        bool ICosmosEventStoreSettings.WithNewStorageIfNotExists { get; set; }

        string IEventStoreSettings.BoundedContext { get; set; }

        DocumentClient ICosmosEventStoreSettings.DocumentClient { get; set; }
    }

    public interface ICosmosEventStoreSettings : IEventStoreSettings
    {
        string DatabaseName { get; set; }
        string CollectionName { get; set; }
        int Throughput { get; set; }
        bool WithNewStorageIfNotExists { get; set; }
        DocumentClient DocumentClient { get; set; }
    }
}
