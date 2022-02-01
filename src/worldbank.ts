import { writeCSVObjects } from "https://deno.land/x/csv/mod.ts";

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

type PopulationRow = {
    indicator: IdValue
    country: IdValue
    countryiso3code: string
    date: string
    value: number
    unit: string
    obs_status: string
    decimal: number
}

type TidyPopulation = {
    country: string
    year: number
    population: number
}

export async function fetchAndSave(path: string): Promise<void> {
    const raw = await fetchPopulation()
    const clean = cleansePopulation(raw)
    await writePopulation(path, clean)
}

async function fetchPopulation(): Promise<PopulationRow[]> {
    const perPage = 2000
    const urlPrefix = `https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&per_page=${perPage}&`

    const firstPageUrl = urlPrefix + 'page=1'
    const res = await fetch(firstPageUrl)
    const [meta, rows] = (await res.json()) as [Meta, PopulationRow[]]
    const allRows = [rows]

    for(let i = 2; i <= meta.pages; i++) {
        const url = urlPrefix + 'page=' + i
        const res = await fetch(url)
        const [_meta, rows] = (await res.json()) as [Meta, PopulationRow[]]
        allRows.push(rows)
    }

    return allRows.flat()
}

function cleansePopulation(data: PopulationRow[]): TidyPopulation[] {
    return data.map(d => ({
        country: d.countryiso3code,
        year: +d.date.substring(0, 4),
        population: +d.value,
    }))
}

async function writePopulation(path: string, data: TidyPopulation[]) {
    const f = await Deno.open(path, {write: true, create: true})
    const header = Object.keys(data[0])
    const generator = async function*() {
        for(let i = 0; i < data.length; i++) yield (data[i] as unknown as Record<string, string>)
    }
    
    await writeCSVObjects(f, generator(), {header})
    f.close()
}