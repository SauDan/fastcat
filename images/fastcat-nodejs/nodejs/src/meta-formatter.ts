import * as fs from 'node:fs/promises';
import * as csv from 'csv-parse';
import * as assert from 'node:assert';



async function fetch_stats(stats_dir: string,
                           sample: string, read_end: string) {
    const filename = `${stats_dir}/${sample}_${read_end}.fastq.stats`;
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


async function* compile_inputs(job_file: string, stats_dir: string) {
    const buffer = await fs.readFile(job_file);
    const job = JSON.parse(buffer.toString());

    const by_sample = new Map<String, {
        end1?: Awaited<ReturnType<typeof fetch_stats>>,
        end2?: Awaited<ReturnType<typeof fetch_stats>>,
    }>();

    for await (const subjob of job.jobs) {
        const { sample, end: read_end } = subjob;
        if (!by_sample.has(sample)) {
            by_sample.set(sample, {});
        }
        const entry = by_sample.get(sample);
        assert(entry != undefined);

        const stats = await fetch_stats(stats_dir, sample, read_end);

        if (read_end == 1) {
            assert(entry.end1 == undefined,
                   `Doubly defined end1 for ${sample}: ${JSON.stringify(stats)}`);
            entry.end1 = stats;
        } else if (read_end == 2) {
            assert(entry.end2 == undefined,
                   `Doubly defined end2 for ${sample}: ${JSON.stringify(stats)}`);
            entry.end2 = stats;
        } else {
            throw new Error("Bug!?");
        }
    }

    for (const [sample, entry] of by_sample) {
        // sanity checks
        assert(entry.end1 != undefined,
               `No end1 for ${sample}`);
        assert(entry.end2 != undefined,
               `No end2 for ${sample}`);
        assert(entry.end1.read_count == entry.end2.read_count,
               `Disagreeing read counts for ${sample}: end1=>${entry.end1.read_count} vs. end2=>${entry.end2.read_count}`);

        yield {
            sample,
            end1_md5: entry.end1.md5,
            end2_md5: entry.end2.md5,
            read_count: entry.end1.read_count,
        }
    }
}


async function format_metadata(data: ReturnType<typeof compile_inputs>,
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

    for await (const row of data) {
        const columns = [
            row.sample,
            `${fastq_s3_prefix}/${row.sample}_1.fastq.gz`, // fastq_forward_path
            `${fastq_s3_prefix}/${row.sample}_2.fastq.gz`, // fastq_reverse_path
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
1. input job file;
2. input stats dir;
3. S3-URL prefix for the FASTQ files;
4. output file`);

    const [ job_file, stats_dir, fastq_s3_prefix, out_file ] = args;

    const data = compile_inputs(job_file, stats_dir);
    return format_metadata(data, fastq_s3_prefix, out_file);
}


main().catch(err => {
    console.dir(err);
    process.exit(1);
});
