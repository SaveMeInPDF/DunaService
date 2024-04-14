namespace DunaService;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

public class Worker : BackgroundService
{
    private readonly string hostname = "localhost";
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        var factory = new ConnectionFactory { HostName = hostname };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }

            await Task.Delay(1000, stoppingToken);
        }
    }
}