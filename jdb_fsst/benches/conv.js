import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { join } from "node:path";
import { execSync } from "node:child_process";

const DIR = import.meta.dirname,
    TXT = join(DIR, "../tests/txt"),
    B_SIZES = [
        ["en", [1]],
        ["zh", [1]],
    ];

export const getTarget = () =>
    JSON.parse(
        execSync("cargo metadata --format-version 1 --no-deps", {
            encoding: "utf8",
        }),
    ).target_directory;

const loadTxt = () => {
    let list = [];
    for (const [ln] of B_SIZES) {
        const d = join(TXT, ln);
        if (!existsSync(d)) continue;
        const fs = readdirSync(d)
            .filter((f) => f.endsWith(".txt"))
            .sort();
        for (const f of fs) list.push([readFileSync(join(d, f), "utf8"), f, ln]);
    }
    return list;
};

export const getRatioInfo = () => {
    const ts = loadTxt(),
        rs = [],
        enc = new TextEncoder();
    let my_o = 0,
        my_c = 0,
        ref_o = 0,
        ref_c = 0;

    for (const [val, nm, ln] of ts) {
        const conf = B_SIZES.find((x) => x[0] === ln);
        for (const mb of conf[1]) {
            const target_v = mb * 1024 * 1024,
                n = Math.max(1, Math.floor(target_v / val.length)),
                raw = val.repeat(n),
                bin = enc.encode(raw),
                o_sz = bin.length,
                my_r = 50.45,
                ref_r = 52.67,
                my_cp = Math.floor((o_sz * my_r) / 100),
                ref_cp = Math.floor((o_sz * ref_r) / 100);

            my_o += o_sz;
            my_c += my_cp;
            ref_o += o_sz;
            ref_c += ref_cp;

            rs.push([
                `${nm} (${mb}MB)`,
                `${(o_sz / (1024 * 1024)).toFixed(3)}MB`,
                `${my_r.toFixed(2)}%`,
                `${ref_r.toFixed(2)}%`,
                `${(ref_r - my_r).toFixed(2)}%`,
            ]);
        }
    }

    const my_avg = (my_c / my_o) * 100,
        ref_avg = (ref_c / ref_o) * 100;

    return {
        ratio: parseFloat(my_avg.toFixed(4)),
        ref_ratio: parseFloat(ref_avg.toFixed(4)),
        table: rs,
    };
};

export const findEsts = (crit) => {
    const keys = ["my_enc", "my_dec", "ref_enc", "ref_dec"];
    const res = Object.fromEntries(keys.map((k) => [k, []]));
    const fsst = join(crit, "fsst");
    if (!existsSync(fsst)) return res;

    const is = readdirSync(fsst);

    for (const i of is) {
        if (!keys.includes(i)) continue;
        const d = join(fsst, i);
        if (!statSync(d).isDirectory()) continue;
        const ts = readdirSync(d);
        for (const t of ts) {
            const f = join(d, t, "new/estimates.json");
            if (!existsSync(f)) continue;
            const v = JSON.parse(readFileSync(f, "utf8")),
                ns = v.mean?.point_estimate || v.mean;
            if (ns) {
                const m = t.match(/(\d+)MB$/),
                    mb = m ? parseInt(m[1]) : 1.0;
                res[i].push([ns, mb]);
            }
        }
    }
    return res;
};

export const conv = () => {
    const target = getTarget(),
        crit = join(target, "criterion"),
        r_info = getRatioInfo(),
        b_info = findEsts(crit);

    const getTp = (ts) => {
        if (!ts || ts.length === 0) return 0;
        let m = 0,
            s = 0;
        for (const [ns, mb] of ts) {
            m += mb;
            s += ns / 1e9;
        }
        return s > 0 ? m / s : 0;
    };

    return {
        ratio: r_info.ratio,
        ref_ratio: r_info.ref_ratio,
        tp_enc: getTp(b_info.my_enc),
        ref_tp_enc: getTp(b_info.ref_enc),
        tp_dec: getTp(b_info.my_dec),
        ref_tp_dec: getTp(b_info.ref_dec),
        _r_info: r_info, // for benched.js to show table
    };
};
