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

export async function fetchAndSave(path: string, indicator: string): Promise<void> {
    const raw = await fetchData(indicator)
    const clean = cleanseData(raw)
    await saveData(path, clean)
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

async function saveData(path: string, data: CleanRow[]) {
    const f = await Deno.open(path, {write: true, create: true})
    const header = Object.keys(data[0])
    const generator = async function*() {
        for(let i = 0; i < data.length; i++) yield (data[i] as unknown as Record<string, string>)
    }
    
    await writeCSVObjects(f, generator(), {header})
    f.close()
}