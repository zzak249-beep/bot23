# 🤖 EMA8 Scalper Bot v4 — BingX Futuros

Bot de trading automático para BingX Futures con triple confirmación temporal,
gestión de riesgo profesional y notificaciones por Telegram.

---

## ✨ Mejoras de Rentabilidad (v4 vs v3)

| Feature | v3 Original | v4 Mejorado |
|---|---|---|
| Confirmación | 3m + 15m | 3m + 15m + **1h EMA200** |
| Stop Loss | % fijo | **ATR dinámico** (se adapta a volatilidad) |
| Filtro de señal | Básico | **Sistema de scoring 0-5** (mín. 3/5) |
| Ratio Riesgo/Recompensa | Sin verificar | **RR mínimo 1.3** garantizado |
| Trailing Stop | No | **Sí** (se activa al 80% del TP1) |
| Drawdown diario | Sin límite | **Máx 5%** (reduce size al 2%) |
| Sesiones | 24/7 | **Filtro de sesión** (Asia/Europa/NY) |
| Resumen diario | No | **Telegram 00:00 UTC** |

---

## 📁 Estructura de Archivos

```
bingx-bot/
├── main.py              # Punto de entrada
├── scanner.py           # Motor de escaneo
├── strategy.py          # Lógica EMA8 + indicadores
├── risk_manager.py      # Gestión de riesgo y posición
├── bingx_api.py         # Cliente HTTP BingX (firmado)
├── telegram_notifier.py # Alertas Telegram
├── config.py            # Configuración central
├── requirements.txt     # Dependencias Python
├── Procfile             # Para Railway
├── railway.toml         # Config Railway
├── .env.example         # Plantilla de variables
└── .gitignore
```

---

## 🚀 Despliegue en Railway (Paso a Paso)

### 1. Preparar GitHub

```bash
# Clonar / crear repositorio
git init
git add .
git commit -m "feat: EMA8 Scalper Bot v4"

# Subir a GitHub
gh repo create ema8-scalper-bot --private
git push -u origin main
```

### 2. Crear proyecto en Railway

1. Ve a [railway.app](https://railway.app) → **New Project**
2. Selecciona **Deploy from GitHub repo**
3. Elige tu repositorio `ema8-scalper-bot`
4. Railway detecta el `Procfile` automáticamente

### 3. Configurar Variables de Entorno en Railway

En tu proyecto Railway → **Variables** → añade una por una:

```
BINGX_API_KEY        = tu_key_real
BINGX_API_SECRET     = tu_secret_real
TELEGRAM_TOKEN       = token_de_botfather
TELEGRAM_CHAT_ID     = tu_chat_id
DRY_RUN              = true          ← empieza siempre en modo prueba
LIQUIDITY_MODE       = high_only
RISK_PER_TRADE       = 1.0
MAX_OPEN_TRADES      = 4
LEVERAGE             = 5
SCAN_INTERVAL_SECONDS = 180
```

### 4. Desplegar

Railway despliega automáticamente al hacer push. También puedes hacer click en **Deploy** manualmente.

---

## 🔑 Obtener Credenciales

### BingX API Key

1. Entra en [bingx.com](https://bingx.com) → **Account** → **API Management**
2. Crea una API Key con permisos: `Read` + `Futures Trading`
3. **Guarda** la Key y Secret (solo se muestran una vez)
4. Pon IP de Railway en whitelist (o deja vacío para cualquier IP)

### Telegram Bot

```
1. Abre Telegram → busca @BotFather
2. Envía: /newbot
3. Escoge nombre y username para tu bot
4. Guarda el TOKEN que te da BotFather

5. Para obtener tu CHAT_ID:
   - Busca @userinfobot en Telegram
   - Envíale cualquier mensaje
   - Te responde con tu Chat ID
```

---

## ⚙️ Configuración de Riesgo Recomendada

### 🟢 Conservador (para empezar)
```
RISK_PER_TRADE = 0.5
LEVERAGE = 3
MAX_OPEN_TRADES = 3
```

### 🟡 Moderado
```
RISK_PER_TRADE = 1.0
LEVERAGE = 5
MAX_OPEN_TRADES = 4
```

### 🔴 Agresivo (solo si tienes experiencia)
```
RISK_PER_TRADE = 2.0
LEVERAGE = 10
MAX_OPEN_TRADES = 6
```

---

## 📊 Cómo Funciona la Estrategia

### Señal LONG (compra) — necesita score ≥ 3/5:

| Puntos | Condición |
|--------|-----------|
| +1 | EMA8 > EMA21 > EMA55 en 3m + precio cruza sobre EMA8 |
| +1 | Precio 15m > EMA21 en 15m (tendencia media alcista) |
| +1 | Precio 1h > EMA200 en 1h (macro alcista) |
| +1 | RSI < 68 (no sobrecomprado) |
| +1 | Higher High reciente + volumen surge o pin bar |

### Stop Loss dinámico (ATR):
```
SL   = precio_entrada - (ATR × 1.5)
TP1  = precio_entrada + (ATR × 2.0)   → RR ≈ 1.33
TP2  = precio_entrada + (ATR × 3.5)   → RR ≈ 2.33
```

### Trailing Stop:
Se activa cuando el precio alcanza el **80% del camino hacia TP1**,
con una distancia del **0.4%** del precio actual.

---

## 🔄 Workflow del Bot

```
cada 3 minutos:
  ↓ ¿sesión activa? (Asia/Europa/NY)
  ↓ obtener símbolos (top 20 alta liquidez)
  ↓ para cada símbolo:
      ↓ descargar velas 3m + 15m + 1h
      ↓ filtro liquidez (vol > 500k USDT/vela)
      ↓ calcular EMAs, RSI, ATR
      ↓ evaluar scoring (0-5)
      ↓ si score ≥ 3 → validar riesgo → ejecutar orden
      ↓ configurar trailing stop
      ↓ notificar Telegram
  ↓ heartbeat cada hora
  ↓ resumen diario a las 00:00 UTC
```

---

## 📱 Mensajes Telegram

El bot envía:
- 🟢 / 🔴 **Señal detectada** (símbolo, precio, SL, TP1, TP2, score)
- ✅ **Orden ejecutada** confirmada
- 💰 / 💸 **Posición cerrada** con PnL
- 💓 **Heartbeat** cada hora (balance + posiciones)
- 📊 **Resumen diario** a medianoche UTC
- ⚠️ **Errores** si algo falla

---

## ⚠️ Advertencias

- **Siempre empieza con `DRY_RUN=true`** para verificar que las señales son correctas
- Verifica al menos **1 semana en modo simulación** antes de activar trading real
- El bot **no garantiza ganancias** — el trading tiene riesgo de pérdida total
- Nunca arriesgues dinero que no puedes permitirte perder
- Monitorea el bot regularmente, no lo dejes completamente desatendido

---

## 🛠️ Desarrollo Local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Copiar .env.example
cp .env.example .env
# Editar .env con tus credenciales reales

# Ejecutar en modo DRY_RUN
DRY_RUN=true python main.py
```

---

## 📝 Licencia

Uso personal. No redistribuir sin autorización.
