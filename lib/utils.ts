export function join(sep: string, ...args: Array<string|null|undefined|false>) {
  return args.filter(Boolean).join(sep);
}
