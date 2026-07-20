FROM mcr.microsoft.com/dotnet/aspnet:9.0-alpine AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:9.0-alpine AS build
WORKDIR /src

COPY ["Aggregator.Host/Aggregator.Host.csproj", "Aggregator.Host/"]
COPY ["Aggregator.Core/Aggregator.Core.csproj", "Aggregator.Core/"]
COPY ["Aggregator.Infrastructure/Aggregator.Infrastructure.csproj", "Aggregator.Infrastructure/"]
COPY ["MockExchanges/MockExchanges.csproj", "MockExchanges/"]

RUN dotnet restore "Aggregator.Host/Aggregator.Host.csproj"
RUN dotnet restore "MockExchanges/MockExchanges.csproj"

COPY . .

FROM build AS publish-aggregator
RUN dotnet publish "Aggregator.Host/Aggregator.Host.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM build AS publish-mock
RUN dotnet publish "MockExchanges/MockExchanges.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS aggregator
WORKDIR /app
COPY --from=publish-aggregator /app/publish .
ENTRYPOINT ["dotnet", "Aggregator.Host.dll"]

FROM base AS mock
WORKDIR /app
COPY --from=publish-mock /app/publish .
ENTRYPOINT ["dotnet", "MockExchanges.dll"]
