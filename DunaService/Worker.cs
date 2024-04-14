using System.Security.Cryptography;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

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
        
        string response = string.Empty;
        var props = ea.BasicProperties;
        var replyProps = channel.CreateBasicProperties();
        replyProps.CorrelationId = props.CorrelationId;
        
        // тут ты работаеш, вызываеш функции, обрабатываешь эту хрень
        
        var body = ea.Body.ToArray();
        // 4 байта - ip, 8 байт - вес файла в байтах, остальное - имя файла и сам файл
        var ip = body[..4];
        var weight = BitConverter.ToInt32(body[4..8]);
        var name = Encoding.UTF8.GetString(body[8..^weight]);
        var file = body[^weight..^1];
        
        // пробросить всё содержимое сообщения через хэш-функцию, таким образом сгенерировать новое имя файла (токен) в 63 символа
        // сохранить файл в папку /files
        // добавить запись в базу данных такого содержания:
        // ip, название файла, токен, размер, UNIX-время, счётчик до удаления (23)
        
        // после того, как ты её обработал, тебе нужно в response записать токен строкой
        var responseBytes = Encoding.UTF8.GetBytes(response);
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
        return BitConverter.ToString(hash).Replace("-", "").ToLower();
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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }
            
            channel.BasicConsume(queue: "hello",
                autoAck: true,
                consumer: consumer);

            await Task.Delay(1000, stoppingToken);
        }
    }
}