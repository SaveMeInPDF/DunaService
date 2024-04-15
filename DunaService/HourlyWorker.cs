namespace DunaService;

using MongoDB.Driver;
using MongoDB.Bson;

class HourlyWorker : BackgroundService
{
    private readonly MongoClient client;
    private IMongoDatabase database;
    private IMongoCollection<BsonDocument> collection;
    private readonly string mongo_address = "mongodb://localhost:27017";
    private readonly ILogger<HourlyWorker> _logger;

    public HourlyWorker(ILogger<HourlyWorker> logger)
    {
        client = new MongoClient(mongo_address);
        database = client.GetDatabase("duna");
        collection = database.GetCollection<BsonDocument>("files");
        
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var filter = Builders<BsonDocument>.Filter.Empty;
            var update = Builders<BsonDocument>.Update.Inc("counter", -1);

            await collection.UpdateManyAsync(filter, update, cancellationToken: stoppingToken);

            // удаляем все элементы, в которых счётчик = 0
            filter = Builders<BsonDocument>.Filter.Eq("counter", 0);
            await collection.DeleteManyAsync(filter, stoppingToken);

            _logger.LogInformation("All counters decremented");

            await Task.Delay(60 * 60 * 1000, stoppingToken);
        }
    }
}