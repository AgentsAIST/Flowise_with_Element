# Flowise_with_Element
### <br>

## [Diagram + DEMO](https://app.holst.so/share/b/6a62eb9a-db48-455b-8a3c-cf1b0d0a8eb1)
[RAG screencast demo](demo/RAG_for_sysadmin.mp4) <br>
[Images recognition screencast demo](demo/ImgBatchAnalyse.mp4)
<br>

## Configure your .env and homeserver.yaml
```
cp .env.template .env
```
<br>

## Launch Synapse with PostgreSQL and Orchestrator services by docker-compose:
```shell
docker compose up -d
```
<br>

## Launch Flowise in local network device or open cloud version
`local e.g:`
```shell
docker run -d --name flowise --network matrix-network -p 3000:3000 -v flowise-data:/data -e FLOWISE_USERNAME=admin -e FLOWISE_PASSWORD=password flowiseai/flowise:latest
```
<br>

## Configure Flowise (Example)
### Launch ollama with cloud model
```shell
docker run -d --network=matrix-network -v ollama:/root/.ollama --name ollama ollama/ollama
```
```shell
docker exec -it ollama ollama signin
```
```shell
docker exec -it ollama ollama run gpt-oss:20b-cloud
```
### Import this [Chatflow](https://github.com/Piankov-Michail/Flowise_with_Element/blob/main/Example_Chatflow.json)
<br>

## RAG (for !rag command using)
### Launch postgres (vector extension) for flowise to keep embeddings
```shell
docker run --name flowise-postgres --network matrix-network -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d pgvector/pgvector:pg17
```
### Launch embeddings model in ollama
```shell
docker exec -it ollama ollama pull mxbai-embed-large
```
### Import this [Chatflow]()
### Otionally to see embeddings launch pgadmin
```shell
docker run --name flowise-pgadmin --network=matrix-network -p 8080:80 -e "PGADMIN_DEFAULT_EMAIL=admin@admin.com" -e "PGADMIN_DEFAULT_PASSWORD=admin" -d dpage/pgadmin4
```

## Open Orchestrator Web client in http://${SERVER_HOST}:8001
### Set up your Element user
### Set up bot Element user
### Set up your bot with bot Element user_id, flowise chatflow_url/agentflow_url and your password to have safe bot control
### Launch your bot
<br>

## Open [Element Web](https://riot.im/app/) or download Element Desktop (Maybe Ethernet bad access)
### Set up your homeserver url
### Log in with your user
### Find bot user `e.g @bot:matrix.local`
### Start chating
<br>

## Resources:
* [Synapse](https://github.com/matrix-org/synapse)
* [Flowise](https://github.com/FlowiseAI/Flowise)
