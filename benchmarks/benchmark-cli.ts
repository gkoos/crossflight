import { runBenchmark } from './benchmark.js'

const args = new Map<string, string>()
for (let index = 2; index < process.argv.length; index += 1) {
  const argument = process.argv[index]
  const match = argument.match(/^--([^=]+)(?:=(.*))?$/)
  if (!match) {
    continue
  }

  const [, name, value = 'true'] = match
  args.set(name, value)
}

await runBenchmark({
  key: args.get('key') ?? 'bench:demo',
  iterations: Number.parseInt(args.get('iterations') ?? '1', 10),
  instances: Number.parseInt(args.get('instances') ?? '5', 10),
  requestsPerInstance: Number.parseInt(args.get('requests-per-instance') ?? args.get('requestsPerInstance') ?? '5', 10),
  concurrency: Number.parseInt(args.get('concurrency') ?? '8', 10),
  loaderMs: Number.parseInt(args.get('loader-ms') ?? args.get('loaderMs') ?? '1000', 10),
  redisUrl: args.get('redis-url') ?? process.env.REDIS_URL ?? 'redis://127.0.0.1:6379',
  resourceMetrics: args.get('resource-metrics') !== 'false' && args.get('resourceMetrics') !== 'false',
})
