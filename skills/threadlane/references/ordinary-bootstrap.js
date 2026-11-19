// Trusted paired ordinary-closure bootstrap. This function body is compiled into
// the native MCP server and must be byte-verified before evaluation.
"use strict";
return function (environment) {
  const {tools, load, store, origin, identity, bootstrap_source} = environment;
  const same = (a, b) => {
    if (a === b) return true;
    if (!a || !b || typeof a !== "object" || typeof b !== "object" || Array.isArray(a) !== Array.isArray(b)) return false;
    const keys = Object.keys(a);
    return keys.length === Object.keys(b).length && keys.every((key) => Object.hasOwn(b, key) && same(a[key], b[key]));
  };
  if (!origin || !identity || identity.bootstrap_verified !== true ||
      !same(identity.origin, origin)) {
    throw new Error("missing verified closure identity");
  }
  if (bootstrap_source !== undefined && typeof bootstrap_source !== "string") {
    throw new Error("invalid ready bootstrap");
  }
  const checked = (r) => {
    const s = r && r.structuredContent;
    if (!s || r.isError || r.is_error || s.untrusted_data !== false ||
        s.tool !== "ordinary_closure_client" || !s.data) throw new Error("closure verification refused");
    for (const x of r.content || []) {
      if (x.type === "text" && !same(JSON.parse(x.text), s)) {
        throw new Error("conflicting closure response");
      }
    }
    return s.data;
  };
  const quote = (v) => "'" + String(v).replace(/'/g, "'\\''") + "'";
  const prefixFor = (id) => {
    if (typeof id !== "string" || !/^apr_[0-9a-f]{32}$/.test(id)) throw new Error("invalid approval ID");
    return "threadlane/closures/" + id + "/";
  };
  async function makeClient(prefix) {
    const result = await tools.exec_command({
      cmd: quote(identity.origin.cli_package + "/bin/threadlane") + " 'closure-template'",
      max_output_tokens: 40000, yield_time_ms: 30000
    });
    if (result.exit_code !== 0 || result.session_id || result.running ||
        result.output_truncated || result.truncated || result.omitted_output ||
        typeof result.output !== "string") throw new Error("canonical closure-template failed");
    const template = JSON.parse(result.output), source = template.javascript;
    if (typeof source !== "string") throw new Error("missing closure source");
    const proof = checked(await tools.mcp__threadlane__ordinary_closure_client({origin, javascript: source}));
    if (proof.source_verified !== true || !same(proof.origin, origin) ||
        Object.keys(template).length !== Object.keys(proof.template).length + 1 ||
        Object.keys(proof.template).some((k) => template[k] !== proof.template[k])) {
      throw new Error("closure source or metadata mismatch");
    }
    const saved = await load(prefix + "origin");
    if (saved != null && !same(saved, origin)) throw new Error("retained closure origin changed");
    if (saved == null) await store(prefix + "origin", origin);
    const makeNative = new Function(source + "\nreturn createOrdinaryNativeClient;")();
    const client = makeNative({tools, load: (k) => load(prefix + k), store: (k, v) => store(prefix + k, v),
      template, cli_package: proof.origin.cli_package, source_sha256: proof.origin.source_sha256});
    const verification = {verified: true, bootstrap_sha256: proof.origin.bootstrap_sha256,
      cli_sha256: proof.origin.cli_sha256, guide_sha256: proof.origin.guide_sha256,
      source_sha256: proof.origin.source_sha256};
    return {client, verification};
  }
  return Object.freeze({
    readNotice: async function (id, options) {
      const prefix = prefixFor(id);
      const {client, verification} = await makeClient(prefix);
      const view = await client.readNotice(id, options);
      if (view.read_id) await store("threadlane/closure-read-index/" + view.read_id, {approval_id: id, origin});
      if (view.status === "ready_for_semantic_review" && bootstrap_source !== undefined) {
        if (!/^rd_[0-9a-f]{64}$/.test(view.read_id)) {
          throw new Error("missing verified ready bootstrap");
        }
        const key = "threadlane/closure-ready/" + view.read_id;
        const ready = {read_id: view.read_id, approval_id: id, origin, bootstrap: bootstrap_source};
        const prior = await load(key);
        if (prior != null && !same(prior, ready)) throw new Error("conflicting ready observation");
        if (prior == null) await store(key, ready);
      }
      view.client_verification = verification;
      return view;
    },
    accept: async function (decision) {
      if (!decision || Object.keys(decision).sort().join(",") !== "read_id,reason,verdict" ||
          decision.verdict !== "accept" || typeof decision.reason !== "string" || !decision.reason.trim() ||
          typeof decision.read_id !== "string" || !/^rd_[0-9a-f]{64}$/.test(decision.read_id)) {
        throw new Error("invalid reviewed read");
      }
      const ready = await load("threadlane/closure-ready/" + decision.read_id);
      if (!ready || ready.read_id !== decision.read_id || !same(ready.origin, origin) ||
          ready.bootstrap !== bootstrap_source) throw new Error("missing or changed ready observation");
      const {client, verification} = await makeClient(prefixFor(ready.approval_id));
      const view = await client.accept({...decision, cli_package: origin.cli_package, source_sha256: origin.source_sha256});
      view.client_verification = verification;
      return view;
    },
    complete: async function (acceptance) {
      if (!acceptance || typeof acceptance.read_id !== "string" || !/^rd_[0-9a-f]{64}$/.test(acceptance.read_id)) {
        throw new Error("invalid semantic acceptance");
      }
      const index = await load("threadlane/closure-read-index/" + acceptance.read_id);
      if (!index || !same(index.origin, origin)) throw new Error("missing original read; reconcile before completion");
      const {client, verification} = await makeClient(prefixFor(index.approval_id));
      const view = await client.complete({...acceptance, cli_package: origin.cli_package, source_sha256: origin.source_sha256});
      view.client_verification = verification;
      return view;
    }
  });
};
