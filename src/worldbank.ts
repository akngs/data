import { writeCSVObjects } from "https://deno.land/x/csv/mod.ts";

const indicators = [
    {
        id: 'SP.POP.TOTL',
        label: 'Total population',
    },
    {
        id: 'NY.GDP.MKTP.CD',
        label: 'GDP (current US $)'
    },
    {
        id: 'NY.GDP.MKTP.PP.CD',
        label: 'GDP (PPP, current internaltional $)'
    },
]

type Meta = {
    page: number
    pages: number
    per_page: number
    total: number
    sourceid: number
    sourcename: string
    lastupdated: string
}

type IdValue = {
    id: string
    value: string
}

type RawRow = {
    indicator: IdValue
    country: IdValue
    countryiso3code: string
    date: string
    value: number
    unit: string
    obs_status: string
    decimal: number
}

type CleanRow = {
    country: string
    year: number
    value: number
}

export async function fetchAndSaveAll(pathPrefix: string): Promise<void> {
    // fetch and save data
    const jobs = indicators.map(i => fetchAndSave(`${pathPrefix}_${i.id}.csv`, i.id))
    await Promise.all(jobs)

    // save metadata
    await saveObjectsAsCsv(`${pathPrefix}_meta.csv`, indicators)
}

export async function fetchAndSave(path: string, indicator: string): Promise<void> {
    const raw = await fetchData(indicator)
    const clean = cleanseData(raw)
    await saveObjectsAsCsv(path, clean)
}

async function fetchData(indicator: string): Promise<RawRow[]> {
    const perPage = 2000
    const urlPrefix = `https://api.worldbank.org/v2/country/all/indicator/${indicator}?format=json&per_page=${perPage}&`

    const firstPageUrl = urlPrefix + 'page=1'
    const res = await fetch(firstPageUrl)
    const [meta, rows] = (await res.json()) as [Meta, RawRow[]]
    const allRows = [rows]

    for(let i = 2; i <= meta.pages; i++) {
        const url = urlPrefix + 'page=' + i
        const res = await fetch(url)
        const [_meta, rows] = (await res.json()) as [Meta, RawRow[]]
        allRows.push(rows)
    }

    return allRows.flat()
}

function cleanseData(data: RawRow[]): CleanRow[] {
    return data.map(d => ({
        country: d.countryiso3code,
        year: +d.date.substring(0, 4),
        value: +d.value,
    }))
}

async function saveObjectsAsCsv(path: string, data: Record<string, any>): Promise<void> {
    const f = await Deno.open(path, {write: true, create: true})
    const header = Object.keys(data[0])
    const generator = async function*() {
        for(let i = 0; i < data.length; i++) yield (data[i] as Record<string, string>)
    }
    await writeCSVObjects(f, generator(), {header})
    f.close()
}