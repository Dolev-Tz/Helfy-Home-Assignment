# SRE Home Assignment

## Prerequisites
- Docker
- Docker Compose

## Running

```bash
docker-compose up --build
```

The app will be available at http://localhost:3000

## Default credentials
- Username: `admin`
- Password: `password123`

## Architecture

- **api** – Node.js / Express backend
- **mysql** – MySQL 8 with binary logging enabled for CDC
- **connect** – Debezium Kafka Connect
- **kafka** – KRaft-mode Kafka broker

## Endpoints

| Method | Path | Auth |
|--------|------|------|
| POST | /login | none |
| GET | /profile | `Authorization: <token>` header |
