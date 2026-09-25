# FURAO PROMO — Telegram público → Discord (Railway + PostgreSQL)

Scraper de canais **públicos** do Telegram (via `https://t.me/s/<canal>`) que publica
ofertas novas em um canal do Discord, rodando automaticamente **1x por hora** no Railway.

## O que mudou em relação à versão original

- A camada de banco de dados foi migrada de **SQLite** para **PostgreSQL**
  (classe `Database` — mesma interface, então nada mais no script precisou mudar).
- O script já possuía a flag `--once`, usada aqui para rodar um ciclo por
  execução do cron (em vez do loop infinito com `time.sleep`).
- `railway.json` configura o comando de start e o agendamento cron.

## Passo a passo no Railway

### 1. Suba o projeto
Crie um repositório no GitHub com estes arquivos e conecte no Railway
(**New Project → Deploy from GitHub repo**), ou use `railway up` pela CLI.

### 2. Adicione o banco PostgreSQL
No projeto do Railway: **New → Database → Add PostgreSQL**.
Isso cria automaticamente a variável `DATABASE_URL`.

### 3. Linke o banco ao serviço do scraper
No serviço do scraper → aba **Variables** → **Add Variable Reference** →
selecione `DATABASE_URL` do serviço Postgres. (Ou copie manualmente o valor.)

### 4. Configure as demais variáveis de ambiente
Ainda em **Variables**, adicione:
- `DISCORD_WEBHOOK_URL` — URL do webhook do seu canal Discord
- `DISCORD_WEBHOOK_USERNAME` — opcional, nome exibido pelo bot

(Veja `.env.example` para referência.)

### 5. Edite `sources.json`
Substitua o exemplo pelos canais reais que você quer monitorar:

```json
[
  {
    "name": "Meu Canal de Ofertas",
    "channel": "nome_do_canal",
    "enabled": true,
    "max_posts": 30,
    "min_price": 0,
    "max_price": "",
    "min_discount_percent": 0,
    "include_keywords": [],
    "exclude_keywords": []
  }
]
```

### 6. Cron já configurado
O `railway.json` já define:
- **Comando:** `python telegram_public_scraper.py --once`
- **Agendamento:** `0 * * * *` (todo início de hora)

Se preferir configurar pela interface em vez do arquivo: **Settings → Cron Schedule**
no serviço, mesma expressão.

> Importante: com Cron Schedule ativo, o Railway roda o comando, aguarda ele
> terminar e desliga o container até a próxima execução — por isso o uso do
> `--once` é essencial (sem ele, o script entraria no loop `while True` e
> nunca soltaria o processo).

### 7. Teste manualmente
Antes de depender só do cron, rode um deploy e dispare manualmente pela aba
**Deployments** (Redeploy) para conferir os logs e garantir que uma oferta de
teste chega no Discord.

## Rodando localmente

```bash
pip install -r requirements.txt
cp .env.example .env   # preencha DISCORD_WEBHOOK_URL e DATABASE_URL
python telegram_public_scraper.py --once --dry-run   # testa sem publicar
python telegram_public_scraper.py --once             # roda 1 ciclo de verdade
```

## Estrutura de arquivos

```
.
├── telegram_public_scraper.py   # script principal (já adaptado p/ Postgres)
├── sources.json                 # canais monitorados (edite com os seus)
├── requirements.txt
├── railway.json                 # comando de start + agendamento cron
├── .env.example
└── README.md
```

## Aviso

Este projeto lê apenas conteúdo **publicamente acessível** (a versão web de
prévia de canais do Telegram), sem login, sem acessar canais/grupos privados
e sem contornar CAPTCHA ou qualquer proteção de conteúdo. Respeite os termos
de uso do Telegram, do Discord e os direitos sobre o conteúdo dos canais
monitorados.
