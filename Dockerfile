FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env
WORKDIR /app

COPY gateway.csproj ./
RUN dotnet restore gateway.csproj

COPY . ./
RUN dotnet publish gateway.csproj -c Release -o out

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app

COPY --from=build-env /app/out .

ARG PUSHOVER_USER_KEY
ARG PUSHOVER_TOKEN_GATEWAY

EXPOSE 8100 8101
ENTRYPOINT [ "dotnet", "gateway.dll", "--urls", "'http://0.0.0.0:8100'" ]