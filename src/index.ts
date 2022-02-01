import {fetchAndSave} from './worldbank.ts'

const indicators = [
    'SP.POP.TOTL',
    'NY.GDP.MKTP.CD',
]
const jobs = indicators.map(i => fetchAndSave(`./build/worldbank_${i}.csv`, i))
await Promise.all(jobs)
