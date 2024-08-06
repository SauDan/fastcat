import * as fs from 'node:fs/promises';
import * as csv from 'csv-parse';
import * as assert from 'node:assert';


// Utility Type
type GeneratorResult<T> = T extends Generator<infer X> ? X : never


async function process_metadata(data: {
    demux_file: string,
    fqlist_file: string,
    input_s3_url_prefix: string,
    job_id: string,
    batch_id: string,
},
                                output_file: string) {
    const [ demux, fqlist ] = await Promise.all([
        fetch_demux(data.demux_file),
        fetch_fqlist(data.fqlist_file),
    ]);

    const by_sample = Array.from(group_metadata_by_sample(demux, fqlist));
    log_summary(by_sample);
    const by_pairend = Array.from(split_by_pairend(by_sample));
    return write_job_details({
        input_s3_url_prefix: data.input_s3_url_prefix,
        job_id: data.job_id,
        batch_id: data.batch_id,
    }, by_pairend, output_file);
}


async function fetch_demux(filename: string) {
    const results = [];
    for await (const entry of parse_demux(filename)) {
        results.push(entry);
    }
    return results;
}

async function fetch_fqlist(filename: string) {
    const results = [];
    for await (const entry of parse_fqlist(filename)) {
        results.push(entry);
    }
    return results;
}


async function* parse_demux(filename: string) {
    const file = await fs.open(filename);
    const parser = file.createReadStream()
        .pipe(csv.parse({ columns: true, }));
    for await (const record of parser) {
        yield {
            sample: record.SampleID as string,
            lane: +record.Lane,
            read_count: +record["# Reads"],
        };
    }
    await file.close();
};

async function* parse_fqlist(filename: string) {
    const file = await fs.open(filename);
    const parser = file.createReadStream()
        .pipe(csv.parse({ columns: true, }));
    for await (const record of parser) {
        yield {
            sample: record.RGSM as string,
            lane: +record.Lane,
            read1_file: record.Read1File as string,
            read2_file: record.Read2File as string,
        }
    }
    await file.close();
};


function* group_metadata_by_sample(demux:  Awaited<ReturnType<typeof fetch_demux>>,
                                   fqlist: Awaited<ReturnType<typeof fetch_fqlist>>){
    const samples: string[] = [];
    const samples_set = new Set<string>();
    demux.map(x => {
        const sample = x.sample;
        if (samples_set.has(sample))
            return;
        samples.push(sample);
        samples_set.add(sample);
    });

    for (const sample of samples) {
        const dm = demux.filter(x => x.sample == sample);
        const fq = fqlist.filter(x => x.sample == sample);
        const by_lane = Array.from(group_metadata_by_lane(dm, fq));

        for (const x of by_lane) {
            x.read_count ||= 0;
            assert(x.read1_file != undefined, `Read1File is undefined for sample ${sample} lane ${x.lane}`);
            assert(x.read2_file != undefined, `Read2File is undefined for sample ${sample} lane ${x.lane}`);
        }

        const read_counts = by_lane.map(x => x.read_count);
        const read1_files = by_lane.map(x => x.read1_file);
        const read2_files = by_lane.map(x => x.read2_file);

        yield {
            sample,
            read1: { counts: read_counts, files: read1_files },
            read2: { counts: read_counts, files: read2_files },
        };
    };
}

function* group_metadata_by_lane(demux:  Awaited<ReturnType<typeof fetch_demux>>,
                                 fqlist: Awaited<ReturnType<typeof fetch_fqlist>>){
    assert(demux.length > 0);
    const sample = demux[0].sample;

    assert(demux.length == fqlist.length,
           `inconsistent number of rows (${demux.length} != ${fqlist.length}) for sample ${sample} in Demultiplex_Stats.csv and fastq_list.csv`);

    demux.sort((a,b) => compare(a.lane, b.lane));
    fqlist.sort((a,b) => compare(a.lane, b.lane));

    for (const i in demux) {
        const dm = demux[i];
        const fq = fqlist[i];
        assert(dm.lane == fq.lane,
               `unmatched lanes (${dm.lane} != ${fq.lane}) for sample ${sample} in Demultiplex_Stats.csv and fastq_list.csv`);

        yield {
            lane: dm.lane,
            read_count: dm.read_count,
            read1_file: fq.read1_file,
            read2_file: fq.read2_file,
        };
    }
}

function compare(x: number, y:number) {
    if (x<y) return -1;
    if (x>y) return +1;
    return 0;
}


function* split_by_pairend(by_sample: GeneratorResult<ReturnType<typeof group_metadata_by_sample>>[]) {
    for (const { sample, read1, read2 } of by_sample) {
        yield {
            sample,
            end: 1,
                ...read1,
        };
        yield {
            sample,
            end: 2,
                ...read2,
        };
    }
}


function log_summary(by_sample: GeneratorResult<ReturnType<typeof group_metadata_by_sample>>[]) {
    console.log(`number of samples: ${by_sample.length}`);
    for (const { sample, read1 } of by_sample) {
        console.log(`  ${sample}: ${read1.files.length} files`)
    }
}


async function write_job_details(job_info :{
    input_s3_url_prefix: string,
    job_id: string,
    batch_id: string
},
                                 by_pairend: GeneratorResult<ReturnType<typeof split_by_pairend>>[],
                                 output_file: string) {
    const file = await fs.open(output_file, 'w');
    file.createWriteStream()
        .write(JSON.stringify({
                ...job_info,
            jobs: by_pairend,
        }, undefined, 2));
    await file.close();
}


async function main() {
    const args = process.argv.slice(2);
    if (args.length != 6)
        throw new Error(`Exactly 6 argument must be given:
1. input Demultiplex_Stats.csv file;
2. input fastq_list.csv file;
3. S3 URL to input FASTQ files;
4. job_id;
5. batch-id;
6. output file.`);

    const [ demux_file, fqlist_file,
            input_s3_url_prefix, job_id, batch_id,
            output_file ] = args;

    return process_metadata({
        demux_file,
        fqlist_file,
        input_s3_url_prefix,
        job_id,
        batch_id,
    }, output_file);
}


main().catch(err => {
    console.dir(err);
    process.exit(1);
});
