import LRUCache from 'lru-cache';
import Redis from 'ioredis';
import msgpackr from 'msgpackr';
import { DogpileLRU } from "./dogpile";
import { join } from './utils';

export interface DogpileRedisOptions<K extends string, V> extends LRUCache.Options<K, V> {
  /**
   * Indicates should we observer all updates.
   * default: true if `redisOptions.keyPrefix` is set, else false
   */
  subscribeAll?: boolean;

  /**
   * Indicates should we persistent updates to Redis.
   * default: true
   */
  persistent?: boolean;

  /**
   * key prefix for persistent.
   * default dereived from `redisOptions.keyPrefix`
   */
  keyPrefix?: string;
  
  pubsubPrefix?: string;

  defaultAge?: number;
}

export class DogpileRedis<V> extends DogpileLRU<string, V> {
  private client: Redis.Redis;
  private pubsub: Redis.Redis;
  private options: DogpileRedisOptions<string, V>;

  constructor(options: DogpileRedisOptions<string, V>, redisOptions: Redis.RedisOptions) {
    const keyPrefix = options.keyPrefix ?? redisOptions.keyPrefix;
    options = {
      ...options,
      subscribeAll: options.subscribeAll || (redisOptions.keyPrefix != null),
      persistent: options.persistent ?? true,
      keyPrefix,
      pubsubPrefix: join(':', keyPrefix, 'update:'),
      defaultAge: 60,
    };
    super(options);
    this.options = options;

    this.client = new Redis(redisOptions);
    this.pubsub = new Redis(redisOptions);
    this.pubsub.on('messageBuffer', this.handleUpdate);

    if (this.options.subscribeAll) {
      void this.pubsub.psubscribe(`${options.pubsubPrefix}*`);
    }
  }

  async getAsync(key: string): Promise<V | undefined> {
    if (!this.options.subscribeAll) {
      void this.pubsub.subscribe(key);
    }
    return super.getAsync(key, async () => {
      const result = await this.client.getBuffer(key as string);
      if (result != null) {
        return msgpackr.unpack(result);
      }
      return undefined;
    })
  }

  set(key: string, value: V, age?: number): boolean {
    console.log(key, value, age);
    age = age ?? this.options.defaultAge;
    void this.setAsync(key, value, age);
    return true;
  }

  async setAsync(key: string, value: V, age?: number): Promise<boolean> {
    const result = super.set(key, value, age);
    if (this.options.persistent) {
      const data = msgpackr.pack({
        data: value,
        age,
      });
      await this.client.setBuffer(key, data, 'EX', age);
      await this.client.publishBuffer(`${this.options.pubsubPrefix}${key}`, data);
    }
    return result;
  }

  private handleUpdate(channel: string, buffer: Buffer) {
    const key = channel.substring(this.options.pubsubPrefix!.length);
    if (this.has(key)) {
      const { data, age } = msgpackr.unpack(buffer);
      this.set(key, data, age > 0 ? age : undefined);
    } else if (!this.options.subscribeAll) {
      void this.pubsub.unsubscribe(`${this.options.pubsubPrefix}${key}`);
    }
  }
}
