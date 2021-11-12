import LRUCache from 'lru-cache';
import Redis from 'ioredis';
import { DogpileLRU } from "./dogpile";
import { join } from './utils';
import msgpackr from 'msgpackr';

export interface DogpileRedisOptions<K extends string, V> extends LRUCache.Options<K, V> {
  /**
   * Indicates should we observer all updates.
   * default: true if `redisOptions.keyPrefix` is set, else false
   */
  subscribeAll: boolean;

  /**
   * Indicates should we persistent updates to Redis.
   * default: true
   */
  persistent: boolean;
}

export class DogpileRedis<V> extends DogpileLRU<string, V> {
  private client: Redis.Redis;
  private pubsub: Redis.Redis;
  private pubsubPrefix: string;
  private subscribeAll: boolean;
  private options: DogpileRedisOptions<string, V>;

  constructor(options: DogpileRedisOptions<string, V>, redisOptions: Redis.RedisOptions) {
    options = {
      ...options,
      persistent: options.persistent ?? true,
    }
    super(options);
    this.options = options;
    
    this.client = new Redis(redisOptions);
    this.pubsub = new Redis(redisOptions);
    this.pubsub.on('messageBuffer', this.handleUpdate);

    this.pubsubPrefix = join(':', redisOptions.keyPrefix, 'update:');
    this.subscribeAll = options.subscribeAll || (redisOptions.keyPrefix != null);

    if (this.subscribeAll) {
      this.pubsub.psubscribe(`${this.pubsubPrefix}*`);
    }
  }

  async getAsync(key: string): Promise<V | undefined> {
    if (!this.subscribeAll) {
      this.pubsub.subscribe(key);
    }
    return super.getAsync(key, async () => {
      const result = await this.client.getBuffer(key as string);
      if (result != null) {
        const unpacked = msgpackr.unpack(result);
        // observe TODO
        return unpacked;
      }
      return undefined;
    })
  }

  set(key: string, value: V, age?: number): boolean {
    const result = super.set(key, value, age);
    if (this.options.persistent) {
      const data = msgpackr.pack({
        data: value,
        age,
      });
      void this.client.setBuffer(key, data, 'EX', age);
      void this.client.publishBuffer(`${this.pubsubPrefix}${key}`, data);
    }
    return result;
  }

  private handleUpdate(channel: string, buffer: Buffer) {
    const key = channel.substring(this.pubsubPrefix.length);
    if (this.has(key)) {
      const { data, age } = msgpackr.unpack(buffer);
      this.set(key, data, age > 0 ? age : undefined);
    } else if (!this.subscribeAll) {
      this.pubsub.unsubscribe(`${this.pubsubPrefix}${key}`);
    }
  }
}
