import { promises as fs } from 'node:fs';
import * as csv from 'csv-parse';
import * as assert from 'node:assert';


async function process_metadata(filename: string, output_dir: string) {
    const metadata = await parse_metadata(filename);
    const by_lib_id = group_metadata_by_lib_id(metadata);
    log_summary(by_lib_id);
    const by_pairend = split_by_pairend(by_lib_id);
    return write_job_details(by_pairend, output_dir);
}


async function parse_metadata(filename: string) {
    const file = await fs.open(filename);
    const parser = file.createReadStream()
        .pipe(csv.parse({
            delimiter: "\t",
            comment: "#",
            comment_no_infix: true,
            columns: true,
        }));
    let records = [];
    for await (const record of parser) {
        records.push(record);
    }
    await file.close();

    return records;
};


function group_metadata_by_lib_id(records: any[]) {
    const lib_ids: Record<string, number> = {};
    records.map((x, i) => {
        const id = x.case_seq_lib_ID;
        if (id in lib_ids)
            return;
        lib_ids[id] = i;
    });
    
    const results: Record<
        string, {
            order: number,
            fastq_forward_path: string[],
            fastq_reverse_path: string[],
            number_of_reads: number[],
        }> = {};
    for (const [lib_id, order] of Object.entries(lib_ids)) {
        const filtered = records.filter(x => x.case_seq_lib_ID === lib_id);
        for (const x of filtered) {
            assert(x.fastq_forward_path != undefined, `fastq_forward_path is undefined in record ${ JSON.stringify(x) }`);
            assert(x.fastq_reverse_path != undefined, `fastq_reverse_path is undefined in record ${ JSON.stringify(x) }`);
        }

        const fastq_forward_path =
            filtered.map((x): string => x.fastq_forward_path);
        const fastq_reverse_path =
            filtered.map((x): string => x.fastq_reverse_path);
        const number_of_reads = filtered.map(x => {
            const n2 = +x.number_of_reads;
            if (n2 == undefined || isNaN(n2))
                return 0;

            assert(n2 % 2 == 0, `expecting an even number, but got ${n2} from record ${ JSON.stringify(x)}`);
            return n2 / 2;
        });

        results[lib_id] = {
            order,
            fastq_forward_path,
            fastq_reverse_path,
            number_of_reads,
        };
    };
    return results;
}


function split_by_pairend(by_lib_id: ReturnType<typeof group_metadata_by_lib_id>) {
    const results: {
        order: number,
        lib_id: string,
        end: number,
        fastq_paths: string[],
        read_counts: number[],
    }[] = [];
    for (const [lib_id, per_lib_id] of Object.entries(by_lib_id)) {
        results.push({
            order: per_lib_id.order,
            lib_id,
            end: 1,
            fastq_paths: per_lib_id.fastq_forward_path,
            read_counts: per_lib_id.number_of_reads,
        });
        results.push({
            order: per_lib_id.order,
            lib_id,
            end: 2,
            fastq_paths: per_lib_id.fastq_reverse_path,
            read_counts: per_lib_id.number_of_reads,
        });
    }
    return results.sort(by_order);
}

function by_order(a: { order: number }, b: { order: number }): number {
    if (a.order < b.order) return -1;
    if (a.order > b.order) return +1;
    return 0;
}

function log_summary(by_lib_id: ReturnType<typeof group_metadata_by_lib_id>) {
    const ordered = Object.entries(by_lib_id).map(([ lib_id, info ]) => {
        return {
            order: info.order,
            lib_id,
            file_count: info.fastq_forward_path.length,
        };
    }).sort(by_order);

    console.log(`number of samples: ${ordered.length}`);
    for (const { lib_id, file_count } of ordered) {
        console.log(`  ${lib_id}: ${file_count} files`)
    }
 }


async function write_job_details(by_pairend: ReturnType<typeof split_by_pairend>,
                                 output_dir: string) {
    async function write_job_data(bpe: typeof by_pairend[number], job_seq: number) {
        const file = await fs.open(`${output_dir}/job.${job_seq}.data`, 'w');
        file.createWriteStream()
            .write(`libid\t${bpe.lib_id}\n` +
                   `end\t${bpe.end}\n`);
    }

    async function write_job_file_list(bpe: typeof by_pairend[number], job_seq: number) {
        const file = await fs.open(`${output_dir}/job.${job_seq}.files`, 'w');
        const out = file.createWriteStream();
        bpe.fastq_paths.map((fastq_path, j) => {
            const n = bpe.read_counts[j];
            out.write(`${n}\t${fastq_path}\n`);
        });
    }
    
    await fs.mkdir(output_dir, {
        recursive: true,
    });
    
    const promises: Promise<void>[] = [];
    by_pairend.map(async (x, i) => {
        promises.push(write_job_data(x, i),
                      write_job_file_list(x, i));
    });

    await Promise.all(promises);
}


async function main() {
    const args = process.argv.slice(2);
    if (args.length != 2)
        throw new Error(`Exactly 2 argument be given:
1. input metadata file;
2. an output directory.`);

    const [ filename, output_dir ] = args;

    return process_metadata(filename, output_dir);
}


main().catch(err => {
    console.dir(err);
    process.exit(1);
});
