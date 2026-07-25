import * as mqtt from 'mqtt';
import { Logger } from 'homebridge';
import BlueAirAwsApi from './BlueAirAwsApi';
import { BlueAirDeviceSensorDataMap, BlueAirDeviceSensorData } from './BlueAirAwsApi';

const AWS_MQTT_BROKERS: Record<string, string> = {
  US: 'a3tpdpjvxk6yog-ats.iot.us-east-2.amazonaws.com',
  EU: 'a3tpdpjvxk6yog-ats.iot.eu-west-1.amazonaws.com',
  AU: 'a3tpdpjvxk6yog-ats.iot.eu-west-1.amazonaws.com',
  CN: 'a2du5f95w7oz2a.ats.iot.cn-north-1.amazonaws.com.cn',
};

const DEFAULT_SENSOR_TTL_SECONDS = 1200;
const RESUBSCRIBE_RATIO = 0.75;

export default class BlueAirMqttClient {
  private client?: mqtt.MqttClient;
  private resubscribeTimer?: NodeJS.Timeout;
  private deviceUuids: string[] = [];

  constructor(
    private readonly api: BlueAirAwsApi,
    private readonly region: string,
    private readonly logger: Logger,
    private readonly onSensorData: (deviceUuid: string, data: BlueAirDeviceSensorData) => void,
  ) {}

  registerDevice(uuid: string): void {
    if (!this.deviceUuids.includes(uuid)) {
      this.deviceUuids.push(uuid);
    }
  }

  async connect(): Promise<void> {
    const broker = AWS_MQTT_BROKERS[this.region];
    if (!broker) {
      throw new Error(`No MQTT broker configured for region: ${this.region}`);
    }

    const creds = this.api.getMqttCredentials();
    if (!creds.authName || !creds.authSignature || !creds.authToken) {
      this.logger.warn('MQTT credentials missing — cannot connect for real-time sensor data');
      return;
    }

    this.logger.info(`Connecting to MQTT broker wss://${broker}`);

    this.client = mqtt.connect(`wss://${broker}:443/mqtt`, {
      clientId: `homebridge-blueair-${Math.random().toString(16).slice(2)}`,
      protocolVersion: 4,
      keepalive: 60,
      reconnectPeriod: 5000,
      wsOptions: {
        headers: {
          'X-Amz-CustomAuthorizer-Name': creds.authName,
          'X-Amz-CustomAuthorizer-Signature': creds.authSignature,
          'X-Amz-CustomAuthorizer-Token': creds.authToken,
        },
      },
    });

    this.client.on('connect', () => {
      this.logger.info('MQTT connected');
      this.subscribeAll();
      this.startResubscribeTimer();
    });

    this.client.on('message', (topic, payload) => {
      this.handleMessage(topic, payload);
    });

    this.client.on('error', (err) => {
      this.logger.warn(`MQTT error: ${err.message}`);
    });

    this.client.on('close', () => {
      this.logger.debug('MQTT connection closed');
      this.cancelResubscribeTimer();
    });

    // Refresh credentials before each reconnect attempt (token expires ~24h)
    this.client.on('reconnect', async () => {
      try {
        await this.api.checkTokenExpiration();
        const fresh = this.api.getMqttCredentials();
        // mqtt.js doesn't expose a clean way to swap wsOptions headers mid-reconnect;
        // simplest reliable approach is to fully reconnect with a new client instance
        // if credentials changed. For now this logs; see note below.
        this.logger.debug(`MQTT reconnecting (auth token present: ${!!fresh.authToken})`);
      } catch (err) {
        this.logger.warn(`Failed to refresh MQTT credentials on reconnect: ${err}`);
      }
    });
  }

  private subscribeAll(): void {
    for (const uuid of this.deviceUuids) {
      this.subscribeDevice(uuid);
    }
  }

  private subscribeDevice(uuid: string): void {
    const topic = `d/${uuid}/s/5s`;
    this.client?.subscribe(topic, (err) => {
      if (err) {
        this.logger.warn(`Failed to subscribe to ${topic}: ${err.message}`);
      } else {
        this.logger.debug(`Subscribed to ${topic}`);
      }
    });
  }

  private startResubscribeTimer(): void {
    this.cancelResubscribeTimer();
    const interval = DEFAULT_SENSOR_TTL_SECONDS * RESUBSCRIBE_RATIO * 1000;
    this.resubscribeTimer = setInterval(() => {
      this.logger.debug('MQTT TTL keepalive: re-subscribing to sensor topics');
      this.subscribeAll();
    }, interval);
  }

  private cancelResubscribeTimer(): void {
    if (this.resubscribeTimer) {
      clearInterval(this.resubscribeTimer);
      this.resubscribeTimer = undefined;
    }
  }

  private handleMessage(topic: string, payload: Buffer): void {
    const match = topic.match(/^d\/([^/]+)\/s\/5s$/);
    if (!match) {
      return;
    }
    const deviceUuid = match[1];

    try {
      const parsed = JSON.parse(payload.toString());
      this.logger.debug(`Raw MQTT sensor payload for ${deviceUuid}: ${JSON.stringify(parsed)}`);

      const sensorData: BlueAirDeviceSensorData = {};
      const list = Array.isArray(parsed) ? parsed : parsed?.n ? [parsed] : [];
      for (const item of list) {
        const key = BlueAirDeviceSensorDataMap[item.n];
        if (key && item.v !== undefined) {
          sensorData[key] = item.v;
        }
      }
      this.onSensorData(deviceUuid, sensorData);
    } catch (err) {
      this.logger.warn(`Failed to parse MQTT payload on ${topic}: ${err}`);
    }
  }

  disconnect(): void {
    this.cancelResubscribeTimer();
    this.client?.end(true);
    this.client = undefined;
  }
}
