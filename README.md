# Paper Trade · Opus + Jev

> **Só simulação.** Nenhum dinheiro real, nenhuma carteira, nenhuma ordem enviada a exchange. É um experimento de agentes de IA tomando decisões sobre preços ao vivo, não um bot de trading nem recomendação de investimento.

Agente de paper trade em cripto: começa com $5.000 simulados, roda 1 hora e usa preços ao vivo da Binance (API pública, sem key). Nenhuma ordem real é enviada.

- **Engine** (a cada 2s): dados, setups, sizing por risco, stop/alvo/trailing, taxa de 0,1% por lado, spread e slippage.
- **Jev** (System One, TypeSafe): gate tipado em toda entrada, inclusive as do Opus. Lê o gráfico descrito em palavras, dá a probabilidade da direção, a qualidade do setup e a chance de exaustão. Também fecha trade que perdeu força.
- **Opus** (System Two, via `claude -p`): decide a cada ~60s. Define a política (risco, pesos, long/short, blacklist) e abre/fecha posições.

```bash
echo TYPESAFE_API_KEY=sua_key > .env
python bot.py        # sessão de 60 min, dashboard em http://localhost:8765
python bot.py 10     # sessão de 10 min
python bot.py test   # self-check do cálculo de PnL/taxas/stops
```

Os logs de cada sessão ficam em `logs/` (JSONL). O dashboard exporta o log e as trades em CSV.

## Mayhem (pump.fun)

`mayhem/` grava e opera em papel as coins Mayhem da pump.fun. Não usa key nem carteira: os dados vêm do RPC público da Solana, com os eventos decodificados pela IDL oficial.

```bash
python mayhem/mayhem.py          # grava em mayhem/data/mayhem.db + paper trade ao vivo, dashboard em http://localhost:8766
python mayhem/mayhem.py replay   # roda as estratégias na fita gravada e compara com o controle aleatório
python mayhem/mayhem.py test     # self-check da matemática da curva e do broker
```

Para usar um RPC melhor: defina `RPC_WS=wss://...` (o público pode ter atraso ou perder mensagens).
