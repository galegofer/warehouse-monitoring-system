## Prerequisites
- Docker
- Docker Compose

---

## Start the system

From the repository root:

```bash
docker compose up --build
```

Services started:

- ActiveMQ (61616, 8161)
- central-service (JMS consumer)
- warehouse-service (UDP consumer, JMS producer)

Send test events
TEMPERATURE (UDP 3344)

```PowerShell
$udp = New-Object System.Net.Sockets.UdpClient
$bytes = [Text.Encoding]::UTF8.GetBytes("sensor_id=h1; value=40")
$udp.Send($bytes, $bytes.Length, "127.0.0.1", 3344) | Out-Null
$udp.Close()
```

Linux / macOS

```bash
echo "sensor_id=h1; value=40" | nc -u -w 1 localhost 3344
```

Expected: central-service logs an ALARM if value > 35.

HUMIDITY (UDP 3355)

```PowerShell
$udp = New-Object System.Net.Sockets.UdpClient
$bytes = [Text.Encoding]::UTF8.GetBytes("sensor_id=h2; value=60")
$udp.Send($bytes, $bytes.Length, "127.0.0.1", 3355) | Out-Null
$udp.Close()
```

Linux / macOS
```bash
echo "sensor_id=h2; value=60" | nc -u -w 1 localhost 3355
```

Invalid payload test

```bash
echo "garbage" | nc -u -w 1 localhost 3344
```

Expected:

warehouse-service: warning
central-service: no alarm
