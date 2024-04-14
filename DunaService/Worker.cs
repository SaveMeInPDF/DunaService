using System.Security.Cryptography;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using MongoDB.Driver;
using MongoDB.Bson;

namespace DunaService;


public class Worker : BackgroundService
{
    private readonly string _hostname = "localhost";
    private readonly ILogger<Worker> _logger;
    private IConnection connection;
    private IModel channel;
    private EventingBasicConsumer consumer;
    private void Received(object? model, BasicDeliverEventArgs? ea)
    {
        if (ea == null) return;
        var props = ea.BasicProperties;
        var replyProps = channel.CreateBasicProperties();
        replyProps.CorrelationId = props.CorrelationId;
        
        // тут ты работаешь, вызываешь функции, обрабатываешь эту хрень
        
        var body = ea.Body.ToArray();
        // 4 байта - ip, 8 байт - вес файла в байтах, остальное - имя файла и сам файл
        var ip = body[..4];
        var weight = BitConverter.ToInt32(body[4..8]);
        var name = Encoding.UTF8.GetString(body[8..^weight]);
        var file = body[^weight..^1];
        
        // пробросить всё содержимое сообщения через хэш-функцию, таким образом сгенерировать новое имя файла (токен) в 64 символа
        // сохранить файл в папку /files
        // добавить запись в базу данных такого содержания:
        // ip, название файла, токен, размер, UNIX-время, счётчик до удаления (24)
        
        // после того как ты её обработал, тебе нужно в response записать токен строкой
        
        SaveFile(name, file);
        var token = Hash(body);
        SaveToDatabase(ip, name, token, weight);
        var responseBytes = Encoding.UTF8.GetBytes(token);
        channel.BasicPublish(exchange: string.Empty,
            routingKey: props.ReplyTo,
            basicProperties: replyProps,
            body: responseBytes);
        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    
    
    private string Hash(byte[] data)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(data);
        return BitConverter.ToString(hash);
    }
    
    private void SaveFile(string name, byte[] data)
    {
        if (!Directory.Exists("files")) Directory.CreateDirectory("files");
        File.WriteAllBytes($"files/{name}", data);
    }
    
    // сохранить данные в таблицу MongoBD
    private void SaveToDatabase(byte[] ip, string name, string token, int weight)
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("duna");
        var collection = database.GetCollection<BsonDocument>("files");
        
        var document = new BsonDocument
        {
            {"ip", new BsonBinaryData(ip)},
            {"name", name},
            {"token", token},
            {"weight", weight},
            {"time", new BsonDateTime(DateTime.UtcNow)},
            {"counter", 24}
        };
        
        collection.InsertOne(document);
    }

    public Worker(ILogger<Worker> logger)
    {
        ConnectionFactory factory = new ConnectionFactory { HostName = _hostname };
        connection = factory.CreateConnection();
        channel = connection.CreateModel();
        _logger = logger;
        
        channel.QueueDeclare(queue: "rpc_queue",
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: null);
        
        channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

        consumer = new EventingBasicConsumer(channel);
        consumer.Received += Received;
    }

    // каждый час проходимся по всем записям в базе данных и уменьшаем счётчик на 1
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("duna");
            var collection = database.GetCollection<BsonDocument>("files");
            
            var filter = Builders<BsonDocument>.Filter.Empty;
            var update = Builders<BsonDocument>.Update.Inc("counter", -1);

            collection.UpdateMany(filter, update);
            
            _logger.LogInformation("All counters decremented");

            await Task.Delay(60*60*1000, stoppingToken);
        }
    }
}