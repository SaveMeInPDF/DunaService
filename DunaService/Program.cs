using DunaService;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();
builder.Services.AddHostedService<HourlyWorker>();

var host = builder.Build();
host.Run();