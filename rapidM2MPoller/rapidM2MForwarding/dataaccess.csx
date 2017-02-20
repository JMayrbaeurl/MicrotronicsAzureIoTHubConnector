#load ".\models\pollingattempt.csx"

using System;
using System.Net;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

public class PollingDB : IDisposable
{
    private string databaseName = "M2MBackendForwarderData";
    private string collectionName = "pollingAttempts";

    private DocumentClient docClient = null;

    public static PollingDB CreateInstance(string dbname, string collName, string dbUri, string dbKey)
    {
        PollingDB result = new PollingDB(dbname, collName);
        result.docClient = new DocumentClient(new Uri(dbUri), dbKey);

        return result;
    }

    public PollingDB (string dbname, string collName)
    {
        this.databaseName = dbname;
        this.collectionName = collName;
    }

    public async Task CreateDatabaseIfNotExists(TraceWriter log)
    {
        if (this.docClient != null)
        {
            // Check to verify a database with the id=FamilyDB does not exist
            try
            {
                await this.docClient.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(this.databaseName));
                log.Info($"Found {this.databaseName}");
            }
            catch (DocumentClientException de)
            {
                // If the database does not exist, create a new database
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await this.docClient.CreateDatabaseAsync(new Database { Id = this.databaseName });
                    log.Info($"Created {this.databaseName}");
                }
                else
                {
                    throw;
                }
            }
        }
    }

    public async Task CreatePollingHistoryCollectionIfNotExists(TraceWriter log)
    {
        try
        {
            await this.docClient.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(this.databaseName, this.collectionName));
            log.Info($"Found {this.collectionName} collection");
        }
        catch (DocumentClientException de)
        {
            // If the document collection does not exist, create a new collection
            if (de.StatusCode == HttpStatusCode.NotFound)
            {
                DocumentCollection collectionInfo = new DocumentCollection();
                collectionInfo.Id = this.collectionName;

                // Configure collections for maximum query flexibility including string range queries.
                collectionInfo.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });

                // Here we create a collection with 400 RU/s.
                await this.docClient.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(this.databaseName),
                    collectionInfo,
                    new RequestOptions { OfferThroughput = 400 });

                log.Info($"Created {this.collectionName} collection");
            }
            else
            {
                throw;
            }
        }
    }

    public PollingAttempt FindLastPollingEntry(string custId, string siteId, TraceWriter log)
    {
        PollingAttempt result = null;

        if (this.docClient != null)
        {
            IQueryable<PollingAttempt> queryResult =
                this.docClient.CreateDocumentQuery<PollingAttempt>(
                    UriFactory.CreateDocumentCollectionUri(this.databaseName, this.collectionName),
                    $"SELECT TOP 1 * FROM pollingAttempts p WHERE p.LastValueFrom != null AND p.Customer_Id = '{custId}' AND p.Site_Id = '{siteId}' ORDER BY p.LastValueFrom DESC");

            if (queryResult != null && queryResult.AsEnumerable().Count() > 0)
            {
                result = queryResult.AsEnumerable().First();
            }
        }

        return result;
    }

    public async Task WritePollingAttemptToDB(PollingAttempt polling, TraceWriter log)
    {
        try
        {
            await this.docClient.ReadDocumentAsync(UriFactory.CreateDocumentUri(this.databaseName,
                this.collectionName, polling.Id));
            log.Info($"Found {polling.Id}");
        }
        catch (DocumentClientException de)
        {
            if (de.StatusCode == HttpStatusCode.NotFound)
            {
                await this.docClient.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(
                    this.databaseName, this.collectionName), polling);
                log.Info($"Created Polling entry {polling.Id}");
            }
            else
            {
                throw;
            }
        }
    }

    ~PollingDB()
    {
        this.CloseDocDBClient();
    }

    public void Dispose()
    {
        this.CloseDocDBClient();
    }

    private void CloseDocDBClient()
    {
        if (this.docClient != null)
        {
            this.docClient.Dispose();
            this.docClient = null;
        }
    }
}