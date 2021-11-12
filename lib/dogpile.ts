import LRUCache from 'lru-cache';

export interface ICallback<R> {
  resolve: (result: R | undefined) => void;
  reject: (error: Error) => void;
}

export interface CacheEntry<T> {
  data: T;
  maxAge?: number;
}

export class DogpileLRU<K, V> extends LRUCache<K, V> {
  private callbacksMap = new Map<K, ICallback<V>[]>();

  constructor(options: LRUCache.Options<K, V>) {
    super(options);
  }
  async getAsync(key: K, filler: () => Promise<CacheEntry<V> | undefined>): Promise<V | undefined> {
    const value = this.get(key);
    if (value) return Promise.resolve(value);

    let callbacks = this.callbacksMap.get(key) as ICallback<V>[];
    if (!callbacks) {
      callbacks = [];
      this.callbacksMap.set(key, callbacks);
      filler()
        .then((result) => {
          callbacks.forEach(cb => {
            if (result && result.maxAge > 0) {
              this.set(key, result.data, result.maxAge);
            }
            if (cb.resolve) {
              process.nextTick(() => cb.resolve(result?.data));
            }
          })
        })
        .catch((error) => {
          callbacks.forEach(cb => {
            if (cb.reject) {
              process.nextTick(() => cb.reject(error));
            }
          })
        })
        .finally(() => {
          this.callbacksMap.delete(key);
        });
    }
    return new Promise((resolve, reject) => {
      callbacks.push({
        resolve,
        reject,
      });
    });
  }
}
