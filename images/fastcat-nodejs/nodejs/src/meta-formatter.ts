import * as fs from 'node:fs/promises';
import * as csv from 'csv-parse';
import * as assert from 'node:assert';
import { StatsFsBase } from 'node:fs';


type SubjobInfo = {
    index: number,
    lib_id: string,
    pair_end: 1 | 2,
};

async function fetch_subjob_info(filename: string)
: Promise<SubjobInfo | undefined> {
    const fh = await fs.open(filename);
    const parser = fh.createReadStream()
        .pipe(csv.parse({ delimiter: "\t" }));
    let lib_id: string | undefined;
    let pair_end: number | undefined ;
    for await (const [ name, value ] of parser) {
        switch (name) {
        case "libid":
            lib_id = value;
            break;
        case "end":
            pair_end = +value;
            break;
        }
    }
    await fh.close();

    assert(pair_end == 1 || pair_end == 2,
           `Invalid 'end': in file ${filename}`);

    if (lib_id != undefined && pair_end != undefined)
        return {
            index: -1,
            lib_id,
            pair_end
        };

    return;
}

async function* gather_subjob_info(dir: string) {
    const dh = await fs.opendir(dir);
    const pattern = /^job\.([0-9]+)\.data$/;
    for (let dir_ent; dir_ent = await dh.read();) {
        const m = dir_ent.name.match(pattern);
        if (!m)
            continue;

        const subjob_info = await fetch_subjob_info(`${dir}/${dir_ent.name}`)
        if (subjob_info != undefined) {
            subjob_info.index = +m[1];
            yield subjob_info;
        }
    }
    await dh.close();
}


type Stats = {
    md5: string,
    read_count: number,
};

async function fetch_subjob_stats(stats_dir: string, subjob_info: SubjobInfo)
: Promise<Stats | undefined> {
    const filename = `${stats_dir}/${subjob_info.lib_id}_${subjob_info.pair_end}.fastq.stats`;
    const fh = await fs.open(filename);
    const parser = fh.createReadStream()
        .pipe(csv.parse({ delimiter: "\t", from: 1, to: 1 }));
    let md5: string | undefined = undefined;
    let read_count: number | undefined = undefined;
    for await (const row of parser) {
        [ md5, read_count ] = row;
    }
    await fh.close();

    if (md5 != undefined && read_count != undefined)
        return { md5, read_count };

    return undefined;
}


async function compile_inputs(job_dir: string, stats_dir: string) {
    let grouped_by_lib_id: Record<string, {
        index: number,
        end1?: Stats,
        end2?: Stats,
    }> = {};

    for await (const subjob_info of gather_subjob_info(job_dir)) {
        const stats = await fetch_subjob_stats(stats_dir, subjob_info);
        if (stats == undefined)
            throw new Error(`cannot fetch MD5 and read count for ${JSON.stringify(subjob_info)}`);

        const { index, lib_id, pair_end } = subjob_info;
        if (!(lib_id in grouped_by_lib_id)) {
            grouped_by_lib_id[lib_id] = { index: -1 };
        }
        const entry = grouped_by_lib_id[lib_id];
        if (pair_end == 1) {
            entry.index = subjob_info.index;
            assert(entry.end1 == undefined,
                   `Doubly defined end1 for ${lib_id}: ${JSON.stringify(stats)}`);
            entry.end1 = stats;
        } else if (pair_end == 2) {
            assert(entry.end2 == undefined,
                   `Doubly defined end2 for ${lib_id}: ${JSON.stringify(stats)}`);
            entry.end2 = stats;
        } else {
            throw new Error("Bug!?");
        }
    }

    // sanity checks
    for (const [ lib_id, entry ] of Object.entries(grouped_by_lib_id)) {
        assert(entry.index >= 0);
        assert(entry.end1 != undefined,
               `No end1 for ${lib_id}`);
        assert(entry.end2 != undefined,
               `No end2 for ${lib_id}`);
        assert(entry.end1.read_count == entry.end2.read_count,
               `Disagreeing read counts for ${lib_id}: end1=>${entry.end1.read_count} vs. end2=>${entry.end2.read_count}`);
    }

    const sorted_lib_ids = Object.keys(grouped_by_lib_id).sort((a, b) => {
        const a_index = grouped_by_lib_id[a].index;
        const b_index = grouped_by_lib_id[b].index;
        if (a_index < b_index) return -1;
        if (a_index > b_index) return +1;
        return 0;
    });

    const flattened = sorted_lib_ids.map(lib_id => {
        const entry = grouped_by_lib_id[lib_id];
        assert(entry.end1);
        assert(entry.end2);
        return {
            lib_id,
            end1_md5: entry.end1.md5,
            end2_md5: entry.end2.md5,
            read_count: entry.end1.read_count,
        }
    });

    return flattened;
}


async function format_metadata(data: Awaited<ReturnType<typeof compile_inputs>>,
                               fastq_s3_prefix: string,
                               out_file: string) {
    const out = (await fs.open(out_file, 'w')).createWriteStream();

    // header
    out.write(`## Metadata for HKGI Sequencing
## case_seq_lib_ID - Name of the sample (Ex - Barcode)
## fastq_forward_path - Forward fastq file path (Ex - Barcode_1.fastq.gz)
## fastq_reverse_path - Reverse fastq file path (Ex - Barcode_2.fastq.gz)
## fastq_forward_md5sum - Forward fastq file md5sum
## fastq_reverse_md5sum - Reverse fastq file md5sum
## number_of_reads - Total number of Reads
## read_length - Each read length
## instrument_platform - Platform used (Ex - ILLUMINA)
## instrument_model - Which Platform model used (Ex - Illumina HiSeq 2000)
## library_layout - Library Layout (Ex - Paired/Single)
## library_strategy - Library strategy used (Ex - WGS/WES)
## library_source - Library source (Ex - DNA/RNA)
## centre_name - Sequencing Centre name
## date - Sequencing upload date
`);

    for (const row of data) {
        const columns = [
            row.lib_id,
            `${fastq_s3_prefix}/${row.lib_id}_1.fastq.gz`, // fastq_forward_path
            `${fastq_s3_prefix}/${row.lib_id}_2.fastq.gz`, // fastq_reverse_path
            row.end1_md5,
            row.end2_md5,
            2 * row.read_count,
            "", // read_length
            "", // instrument_platform
            "", // instrument_model
            "", // library_layout
            "", // library_strategy
            "", // library_source
            "", // centre_name
            "", // date
        ];
        out.write(columns.join("\t") + "\n");
    }
    return out.close();
}


async function main() {
    const args = process.argv.slice(2);
    if (args.length != 4)
        throw new Error(`Exactly 4 argument be given:
1. input job info dir;
2. input stats dir;
3. S3-URL prefix for the FASTQ files;
4. output file`);

    const [ job_dir, stats_dir, fastq_s3_prefix, out_file ] = args;

    const data = await compile_inputs(job_dir, stats_dir);
    return format_metadata(data, fastq_s3_prefix, out_file);
}


main().catch(err => {
    console.dir(err);
    process.exit(1);
});
