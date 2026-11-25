"""ci_run.py helpers: the Loki metric wait and the first-pass difference list."""
import argparse
import http.server
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import ci_run  # noqa: E402


class FakeLoki(http.server.BaseHTTPRequestHandler):
    answers = []

    def do_GET(self):
        body = json.dumps({"data": {"result": self.answers.pop(0) if self.answers else []}}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


class CiRunTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Node.js is required for capture DOM unit tests")
    def test_ui_errors_are_not_log_sample_contents(self):
        source = Path(ci_run.HERE, "capture.spec.ts").read_text().split("async function uiState(", 1)[1].split("// Grafana's", 1)[0]
        node = r'''const assert = require("node:assert/strict");
const strip = require("node:module").stripTypeScriptTypes;
if (!strip) process.exit(77);
const source = require("node:fs").readFileSync(0, "utf8");
const uiState = Function(strip(source) + ";return uiState")();
global.NodeFilter = {SHOW_TEXT: 4, SHOW_ELEMENT: 1};
const element = (text, data = null, visible = true, block = {}) => ({
  textContent: text, getClientRects: () => visible ? [1] : [],
  closest: selectors => selectors.startsWith('address,') ? block :
    data && selectors.split(',').some(s => s.trim() === data) ? {} : null
});
async function check({alerts = [], nodes = [], expected = [], panels = []}) {
  const text = [...alerts, ...nodes.map(n => n.parentElement)].filter(Boolean).map(e => e.textContent).join("\n");
  global.document = {
    body: {innerText: text},
    querySelectorAll: s => s.startsWith('[role="alert"]') ? alerts : s === "details" ? [] : panels,
    createTreeWalker: () => {
      let i = -1;
      return {nextNode() { return ++i < nodes.length; }, get currentNode() { return nodes[i]; }};
    }
  };
  const result = await uiState({evaluate: fn => Promise.resolve(fn())});
  assert.deepEqual(result.banners, expected);
  assert.equal(result.panelErrors, panels.filter(p => p.getClientRects().length).length);
}
const textNode = parentElement => ({parentElement, textContent: parentElement.textContent});
(async () => {
  await check({nodes: [textNode(element('{"error":"TypeError: Cannot read property", "level":"error"}', '[data-log-index]')),
                      textNode(element('Plugin unavailable', '.log-line-body'))]});
  await check({alerts: [element('Cannot read properties of undefined')],
               expected: ['Cannot read properties of undefined']});
  await check({alerts: [element('Plugin unavailable', false, false)]});
  await check({nodes: [textNode(element('Plugin failed to load'))], expected: ['Plugin failed to load']});
  await check({nodes: [textNode(element('An unexpected error occurred'))], expected: ['An unexpected error occurred']});
  const block = {};
  await check({nodes: [textNode(element('Plugin ', null, true, block)), textNode(element('unavailable', null, true, block))],
               expected: ['Plugin unavailable']});
  await check({nodes: [textNode(element('Plugin ', null, true, block)), textNode(element('hidden', null, false, block)),
                      textNode(element('unavailable', null, true, block))], expected: ['Plugin unavailable']});
  await check({nodes: [textNode(element('Plugin ', null, true, block)),
                      {...element(''), nodeType: 1, tagName: 'BR', parentElement: null},
                      textNode(element('unavailable', null, true, block))]});
  await check({nodes: [textNode(element('Plugin ')), textNode(element('unavailable'))]});
  await check({nodes: [textNode(element('Plugin ', null, true, block)), textNode(element('log', '[data-log-index]')),
                      textNode(element('unavailable', null, true, block))]});
  await check({nodes: [textNode(element('Cannot read properties of undefined', '[data-testid="data-testid Panel content"]'))],
               expected: ['Cannot read properties of undefined']});
  await check({alerts: [element('Query error: Data source became unavailable')],
               expected: ['Query error: Data source became unavailable'], panels: [element('error')]});
})().catch(e => {console.error(e); process.exit(1);});'''
        proc = subprocess.run(["node", "-e", node], input="async function uiState(" + source,
                              text=True, capture_output=True)
        if proc.returncode == 77:
            self.skipTest("Node 22.13+ supplies the TypeScript loader")
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_runtime_boundary_precedes_recording_overhead(self):
        source = Path(ci_run.HERE, "capture.spec.ts").read_text()
        after_settle = source.split("const ok = await settle(page, pending);")[1]
        drain = after_settle.index("await drain(reading)")
        self.assertLess(after_settle.index("Date.now() - t0"), drain)
        self.assertLess(after_settle.index("runtimeProof.finish("), drain)
        self.assertLess(drain, after_settle.index("await page.screenshot"))

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for Chromium collector unit tests")
    def test_independent_body_collector_requires_unique_complete_identity(self):
        source = Path(ci_run.HERE, "capture.spec.ts").read_text().split("const BACKEND = ", 1)[1].split("// Raw pageerror", 1)[0]
        node = r"""const assert = require("node:assert/strict");
const {EventEmitter} = require("node:events");
const strip = require("node:module").stripTypeScriptTypes;
if (!strip) process.exit(77);
const start = Function("createHash", strip(require("node:fs").readFileSync(0,"utf8"))+";return startBodyFallback")(require("node:crypto").createHash);
const identity = {method:"POST",url:"http://localhost/api/ds/query?requestId=1",postData:'{"queries":[{"expr":"rate({app=\\"api\\"}[5m])"}]}',status:200};
function fixture() {
  const cdp=new EventEmitter(); let calls=0;
  cdp.send=async(method,args)=>{assert.equal(method,"Network.getResponseBody");assert.equal(args.requestId,"one");calls++;return{body:'{"data":[1]}',base64Encoded:false};};
  return {cdp,collector:start(cdp),calls:()=>calls};
}
function add(cdp,id="one",over={}) {
  cdp.emit("Network.requestWillBeSent",{requestId:id,request:{method:identity.method,url:identity.url,postData:identity.postData,...over}});
  cdp.emit("Network.responseReceived",{requestId:id,response:{url:identity.url,status:200}});
  cdp.emit("Network.loadingFinished",{requestId:id});
}
(async()=>{
  let f=fixture();add(f.cdp);
  let got=await f.collector.read(identity);assert.equal(got.body.toString(),'{"data":[1]}');assert.equal(f.calls(),1);f.collector.dispose();
  for(const changed of [{method:"GET"},{url:identity.url+"&other=1"},{postData:identity.postData+" "},{status:201}]) {
    f=fixture();add(f.cdp);got=await f.collector.read({...identity,...changed});
    assert.equal(got.body,undefined);assert.match(got.error,/identity|status/);assert.equal(f.calls(),0);f.collector.dispose();
  }
  f=fixture();add(f.cdp);add(f.cdp,"two");got=await f.collector.read(identity);
  assert.equal(got.body,undefined);assert.match(got.error,/ambiguous/);assert.equal(f.calls(),0);f.collector.dispose();
  f=fixture();add(f.cdp);f.cdp.emit("Network.requestWillBeSent",{requestId:"one",redirectResponse:{status:302},request:{method:"POST",url:identity.url,postData:identity.postData}});
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/redirect/);assert.equal(f.calls(),0);f.collector.dispose();
  f=fixture();add(f.cdp);
  f.cdp.emit("Network.requestWillBeSent",{requestId:"one",redirectResponse:{status:302},request:{method:"POST",url:identity.url,postData:identity.postData}});
  const longRedirectURL="http://localhost/redirect?value="+"x".repeat(4*1024*1024+1);
  let redirectedMetadataReads=0;
  f.cdp.emit("Network.responseReceived",{requestId:"one",response:{
    get url(){redirectedMetadataReads++;return longRedirectURL;},
    get status(){redirectedMetadataReads++;return 200;}
  }});
  assert.equal(redirectedMetadataReads,0,"redirect metadata must not be read or retained");
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/redirect/);assert.equal(f.calls(),0);f.collector.dispose();
  f=fixture();add(f.cdp);f.cdp.emit("Network.loadingFailed",{requestId:"one",canceled:true});
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/failed|incomplete/);assert.equal(f.calls(),0);f.collector.dispose();
  f=fixture();f.cdp.emit("Network.requestWillBeSent",{requestId:"one",request:{method:"POST",url:identity.url,hasPostData:true}});
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.equal(f.calls(),0);f.collector.dispose();
  f=fixture();add(f.cdp);f.cdp.send=async()=>{throw Error("inspector unavailable");};
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/unavailable/);f.collector.dispose();
  f=fixture();add(f.cdp);f.cdp.send=async()=>({body:"x".repeat(32*1024*1024+1),base64Encoded:false});
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/limit/);f.collector.dispose();
  f=fixture();add(f.cdp);f.cdp.send=async()=>({body:"%%%=",base64Encoded:true});
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/encoding/);f.collector.dispose();
  f=fixture();add(f.cdp);f.cdp.send=async()=>({body:Buffer.from('{"data":[1]}').toString("base64"),base64Encoded:true});
  got=await f.collector.read(identity);assert.equal(got.body.toString(),'{"data":[1]}');f.collector.dispose();
  f=fixture();add(f.cdp);f.cdp.send=async()=>({base64Encoded:false});
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/unavailable/);f.collector.dispose();
  f=fixture();add(f.cdp);f.cdp.emit("Network.responseReceived",{requestId:"one",response:{url:identity.url+"&wrong=1",status:200}});
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/identity/);assert.equal(f.calls(),0);f.collector.dispose();
  // A body over the 32 MiB the capture retains is not handed over, but it is read whole and hashed, so the page can prove its answer complete.
  f=fixture();add(f.cdp);const big='{"results":{"A":{"status":200,"frames":[{"x":"'+"y".repeat(33*1024*1024)+'"}]}}}';
  f.cdp.send=async()=>({body:big,base64Encoded:false});
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/limit/);
  assert.equal(got.digest.bytes,Buffer.byteLength(big));assert.equal(got.digest.sha256,require("node:crypto").createHash("sha256").update(big).digest("hex"));
  assert.equal(got.digest.head.length,16*1024);assert.ok(got.digest.head.startsWith('{"results"'));assert.equal(got.digest.tail,big.slice(-4*1024));f.collector.dispose();
  // The same body in base64, and one over the digest bound, which is only reported as over the limit.
  f=fixture();add(f.cdp);f.cdp.send=async()=>({body:Buffer.from(big).toString("base64"),base64Encoded:true});
  got=await f.collector.read(identity);assert.match(got.error,/limit/);assert.equal(got.digest.bytes,Buffer.byteLength(big));f.collector.dispose();
  f=fixture();add(f.cdp);f.cdp.emit("Network.requestWillBeSent",{requestId:"large",request:{method:"POST",url:identity.url,postData:"x".repeat(256*1024+1)}});
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/limit/);assert.equal(f.calls(),0);f.collector.dispose();
  f=fixture();for(let i=0;i<1025;i++)add(f.cdp,"request-"+i);
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/limit/);assert.equal(f.calls(),0);f.collector.dispose();
  f=fixture();add(f.cdp);let release;
  f.cdp.send=()=>new Promise(resolve=>{release=resolve;});
  const first=f.collector.read(identity), second=f.collector.read(identity);
  got=await f.collector.read(identity);assert.equal(got.body,undefined);assert.match(got.error,/concurrent/);
  f.collector.dispose();assert.match((await first).error,/disposed/);assert.match((await second).error,/disposed/);
  f=fixture();add(f.cdp);f.collector.dispose();got=await f.collector.read(identity);
  assert.equal(got.body,undefined);assert.match(got.error,/disposed/);assert.equal(f.calls(),0);
  for(const event of ["Network.requestWillBeSent","Network.responseReceived","Network.loadingFinished","Network.loadingFailed"])assert.equal(f.cdp.listenerCount(event),0);
})().catch(error=>{console.error(error);process.exit(1);});
"""
        proc = subprocess.run(["node", "--max-old-space-size=192", "-e", node], input="const BACKEND = " + source,
                              text=True, capture_output=True, timeout=20)
        if proc.returncode == 77:
            self.skipTest("Node 22.13+ supplies the TypeScript loader")
        self.assertEqual(proc.returncode, 0, proc.stderr)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for Chromium collector unit tests")
    def test_answer_fallback_preserves_cancelled_and_unavailable_records(self):
        source = Path(ci_run.HERE, "capture.spec.ts").read_text()
        answer = source.split("const cancelled = ", 1)[1].split("for (const p of spec.pages", 1)[0]
        node = r"""const assert=require("node:assert/strict");
const strip=require("node:module").stripTypeScriptTypes;
if(!strip)process.exit(77);
const record=Function(strip(require("node:fs").readFileSync(0,"utf8"))+";return recordAnswer")();
let calls=0, failure=null;
const rq={url:()=>"http://localhost/api/ds/query?requestId=1",method:()=>"POST",postData:()=>'{"queries":[1]}',postDataJSON:()=>({queries:[1]}),failure:()=>failure};
const response=body=>({request:()=>rq,status:()=>200,headers:()=>({"content-length":"12"}),body,
  json:async()=>{throw Error("Request content was evicted from inspector cache");},text:async()=>{throw Error("Request content was evicted from inspector cache");}});
const recover=async identity=>{calls++;assert.deepEqual(identity,{method:"POST",url:rq.url(),postData:rq.postData(),status:200});return{body:Buffer.from('{"data":[1]}')};};
(async()=>{
 const over=async()=>{};
 let got=await record(response(async()=>{throw Error("Request content was evicted from inspector cache");}),over,recover);
 assert.deepEqual(got.response,{data:[1]});assert.equal(got.body_source,"cdp-fallback");assert.equal(calls,1);
 got=await record(response(async()=>Buffer.from('{"data":[2]}')),over,recover);
 assert.deepEqual(got.response,{data:[2]});assert.equal(got.body_source,undefined);assert.equal(calls,1);
 got=await record(response(async()=>Buffer.from("plain error")),over,recover);
 assert.equal(got.response,"plain error");assert.equal(calls,1);
 failure={errorText:"net::ERR_ABORTED"};
 got=await record(response(async()=>{throw Error("Request content was evicted from inspector cache");}),over,recover);
 assert.equal(got.status,0);assert.equal(got.aborted,"net::ERR_ABORTED");assert.equal(calls,1);
 failure=null;
 got=await record(response(async()=>{throw Error("Target page closed");}),over,recover);
 assert.equal(got.response,null);assert.match(got.body_unavailable,/Target page closed/);assert.equal(calls,1);
 got=await record(response(async()=>{throw Error("Request content was evicted from inspector cache");}),over,async()=>({error:"ambiguous request identity"}));
 assert.equal(got.response,null);assert.match(got.body_unavailable,/ambiguous/);
})().catch(error=>{console.error(error);process.exit(1);});
"""
        proc = subprocess.run(["node", "-e", node], input="const cancelled = " + answer,
                              text=True, capture_output=True, timeout=20)
        if proc.returncode == 77:
            self.skipTest("Node 22.13+ supplies the TypeScript loader")
        self.assertEqual(proc.returncode, 0, proc.stderr)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for Chromium collector unit tests")
    def test_exception_collector_binds_actual_events_and_fails_closed(self):
        source = Path(ci_run.HERE, "capture.spec.ts").read_text().split("const BACKEND = ", 1)[1].split("// Logs Drilldown", 1)[0]
        node = r'''const assert = require("node:assert/strict");
const {EventEmitter} = require("node:events");
const strip = require("node:module").stripTypeScriptTypes;
if (!strip) process.exit(77);
const source = require("node:fs").readFileSync(0, "utf8");
const start = Function(strip(source) + ";return startRuntimeProof")();
const props = [{name:'refId',value:{type:'string',value:'log-volume-A'}},
               {name:'message',value:{type:'string',value:'parse error: unexpected SUM'}},
               {name:'status',value:{type:'number',value:400}}];
const event = id => ({exceptionDetails:{exceptionId:id,text:'Uncaught',
  stackTrace:{callFrames:[{url:'http://localhost/public/build/client.js',lineNumber:1,columnNumber:2}]},
  exception:{type:'object',className:'Object',description:'Object',objectId:'object-'+id,
    preview:{overflow:false,properties:props.map(p=>({name:p.name,type:p.value.type,value:String(p.value.value)}))}}}});
async function run(mode) {
  const cdp = new EventEmitter(); let detached=0, propertyReads=0;
  cdp.send = async (method, parameters) => {
    if (method === 'Runtime.getProperties') {
      propertyReads++; assert.equal(parameters.ownProperties,true);
      if (mode === 'timeout') return new Promise(()=>{});
      return {result: mode === 'accessor' ? [{name:'refId',get:{type:'function'}},...props.slice(1)] : props};
    }
    return {metrics:[{name:'JSHeapUsedSize',value:16}]};
  };
  cdp.detach = async () => {detached++;};
  const page = new EventEmitter(); page.context = () => ({newCDPSession:async()=>cdp}); page.isClosed=()=>false;
  const errors=[];
  const proof = await start(page, Date.now(), errors);
  if (mode === 'close') {page.emit('close'); await new Promise(r=>setImmediate(r)); assert.equal(detached,1); return;}
  const count=mode==='overflow'?5:1;
  for(let id=1;id<=count;id++) {cdp.emit('Runtime.exceptionThrown',event(id)); page.emit('pageerror',new Error('Object'));}
  await proof.finish();
  const state=await proof.finishClientExceptions();
  assert.equal(detached,1); assert.equal(page.listenerCount('pageerror'),0);
  assert.equal(cdp.listenerCount('Runtime.exceptionThrown'),0);
  if(mode==='normal') {
    assert.equal(state.complete,true);assert.equal(state.overflow,false);
    assert.equal(state.events[0].event_id,'runtime:1');assert.equal(state.events[0].raw_error_index,0);
    assert.equal(errors[0],'pageerror: Object');assert.equal(state.playwright_page_error_count,1);
    assert.deepEqual(state.events[0].own_property_names,['message','refId','status']);
    assert.equal(state.events[0].ref_id,'log-volume-A');assert.equal(state.events[0].status,400);
  } else assert.equal(state.complete,false,mode);
  if(mode==='overflow') {assert.equal(state.events.length,4);assert.equal(state.overflow,true);}
  assert.ok(propertyReads<=4);
}
(async()=>{for(const mode of ['normal','accessor','overflow','timeout','close']) await run(mode);})()
 .catch(e=>{console.error(e);process.exit(1)});'''
        proc = subprocess.run(["node", "-e", node], input="const BACKEND = " + source,
                              text=True, capture_output=True, timeout=20)
        if proc.returncode == 77:
            self.skipTest("Node 22.13+ supplies the TypeScript loader")
        self.assertEqual(proc.returncode, 0, proc.stderr)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for Chromium collector unit tests")
    def test_redirect_hops_and_first_metadata_data_in_the_runtime_collector(self):
        # Run the capture's collector against CDP events without Chromium, network or Playwright.
        source = Path(ci_run.HERE, "capture.spec.ts").read_text().split("const BACKEND = ", 1)[1].split("// Logs Drilldown", 1)[0]
        node = """const assert = require("node:assert/strict");
const strip = require("node:module").stripTypeScriptTypes;
if (!strip) process.exit(77);
const source = require("node:fs").readFileSync(0, "utf8");
const start = Function(strip(source) + ";return startRuntimeProof")();
(async () => {
  for (const response of [{data: ["service"]}, {fields: [{label: "service"}]}, {data: {result:[1]}}, {data: []}]) {
    const cdp = new (require("node:events").EventEmitter)();
    cdp.send = async () => ({metrics: [{name: "JSHeapUsedSize", value: 16}]});
    cdp.detach = async () => {};
    const page = new (require("node:events").EventEmitter)();
    page.context = () => ({newCDPSession: async () => cdp}); page.isClosed = () => false;
    const proof = await start(page);
    cdp.emit("Network.requestWillBeSent", {requestId: "1", request: {url: "/api/ds/query"}});
    cdp.emit("Network.dataReceived", {requestId: "1", encodedDataLength: 20});
    cdp.emit("Network.requestWillBeSent", {requestId: "1", request: {url: "/app"}, redirectResponse: {encodedDataLength: 70}});
    cdp.emit("Network.loadingFinished", {requestId: "1", encodedDataLength: 120});
    proof.recordData([{status: 200, response}]);
    const measured = await proof.finish();
    await proof.finishClientExceptions();
    if (process.argv[1] === "redirect") {
      assert.equal(measured.api_encoded_bytes, 70);
      assert.equal(measured.page_encoded_bytes, 190);
      assert.equal(measured.api_requests, 1);
    } else {
      assert.equal(measured.first_api_data_ms !== undefined, !(Array.isArray(response.data) && !response.data.length));
    }
  }
})().catch(e => {console.error(e); process.exit(1);});"""
        for case in ("redirect", "metadata"):
            with self.subTest(case=case):
                proc = subprocess.run(["node", "-e", node, case], input="const BACKEND = " + source,
                                      text=True, capture_output=True)
                if proc.returncode == 77:
                    self.skipTest("Node 22.13+ supplies the TypeScript loader")
                self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_warm_recapture_cannot_clear_first_pass_resource_regression(self):
        first = [{"page": "dd-fields", "range": "1h", "runtime_problems": ["runtime regression: browser thread CPU 1 -> 2"],
                  "runtime_main": {"browser_thread_seconds": 1}, "runtime_pr": {"browser_thread_seconds": 2}}]
        second = [{"page": "dd-fields", "range": "1h", "runtime_problems": [],
                   "runtime_main": {"browser_thread_seconds": 0.1}, "runtime_pr": {"browser_thread_seconds": 0.1}}]
        got = ci_run.keep_runtime_observations(first, second)[0]
        self.assertIn("first pass retained", got["runtime_problems"][0])
        self.assertEqual(got["runtime_first_pass"]["runtime_pr"]["browser_thread_seconds"], 2)
        self.assertEqual(got["runtime_pr"]["browser_thread_seconds"], 0.1)

    def test_wait_loki_metric_waits_through_empty_answers(self):
        FakeLoki.answers = [[], [{"values": [[1, "0"]]}], [{"values": [[1, "7"]]}]]
        srv = http.server.HTTPServer(("127.0.0.1", 0), FakeLoki)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        orig = ci_run.time.sleep
        ci_run.time.sleep = lambda s: None  # the polling pause, not the logic, is slow
        try:
            self.assertIsNotNone(ci_run.wait_loki_metric(srv.server_address[1], 1000, deadline_s=30))
            self.assertEqual(FakeLoki.answers, [])  # it kept asking until non-zero data appeared
            FakeLoki.answers = []
            self.assertIsNone(ci_run.wait_loki_metric(srv.server_address[1], 1000, deadline_s=0))
        finally:
            ci_run.time.sleep = orig
            srv.shutdown()

    def test_the_seed_fingerprint_counts_both_backends_and_tolerates_a_dead_one(self):
        class Both(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = (b'{"c":"42"}\n' if "logsql" in self.path else
                        json.dumps({"data": {"result": [{"value": [1, "17"]}]}}).encode())
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", 0), Both)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            port = srv.server_address[1]
            state = {"end": 7200, "hours": 7, "loki_hours": 1.5}
            self.assertEqual(ci_run.seed_fingerprint(state, port, port), {"end": 7200, "hours": 7, "loki_hours": 1.5, "vl_lines": 42, "loki_lines": 17})
            dead = ci_run.seed_fingerprint(state, 1, 1)
            self.assertEqual((dead["vl_lines"], dead["loki_lines"]), (None, None))
        finally:
            srv.shutdown()

    def test_the_first_pass_of_a_capture_loaded_again_is_kept_apart(self):
        try:
            from PIL import Image
        except ImportError:  # the montage step needs Pillow (bench/visual/requirements.txt); the lint job has none
            self.skipTest("Pillow is not installed")
        with tempfile.TemporaryDirectory() as out:
            for page in ("again", "once"):
                for name in ("main", "pr", "loki"):
                    d = os.path.join(out, "shots", page, "1h")
                    os.makedirs(d, exist_ok=True)
                    Image.new("RGB", (40, 30), (200, 10, 10) if name == "pr" else (10, 10, 200)).save(os.path.join(d, f"{name}.png"))
            with open(os.path.join(out, "compare.json"), "w", encoding="utf-8") as f:
                json.dump([{"page": "again", "range": "1h", "main_pr_diffs": ["x"]}, {"page": "once", "range": "1h", "main_pr_diffs": []}], f)
            ci_run.keep_first_pass(out, {"again": {"kind": "explore", "ranges": ["1h"]}})
            first = os.path.join(out, "first-pass")
            with open(os.path.join(first, "compare.json"), encoding="utf-8") as f:
                self.assertEqual([r["page"] for r in json.load(f)], ["again"])
            self.assertEqual(os.listdir(os.path.join(first, "montage")), ["again-1h.png"])
            with open(os.path.join(first, "pixeldiff.json"), encoding="utf-8") as f:
                self.assertEqual(list(json.load(f)), ["again-1h"])

    def test_differing_lists_the_captures_with_a_difference(self):
        with tempfile.TemporaryDirectory() as out:
            self.assertEqual(ci_run.differing(out), set())
            with open(os.path.join(out, "compare.json"), "w", encoding="utf-8") as f:
                json.dump([{"page": "a", "range": "1h", "main_pr_diffs": ["x"]}, {"page": "b", "range": "1h", "main_pr_diffs": []}], f)
            self.assertEqual(ci_run.differing(out), {"a 1h"})

    def test_a_delayed_request_that_matches_on_recapture_is_not_a_data_flip(self):
        first = [
            {"page": "late", "range": "1h", "main_pr_diffs": ["query x: request only on one side (left)"]},
            {"page": "value", "range": "1h", "main_pr_diffs": ["query x: value differs"]},
            {"page": "mixed", "range": "1h", "main_pr_diffs": ["query x: request only on one side (left)", "query y: value differs"]},
            {"page": "persistent", "range": "1h", "main_pr_diffs": ["query x: value differs"]},
        ]
        second = [{**r, "main_pr_diffs": r["main_pr_diffs"] if r["page"] == "persistent" else []} for r in first]
        self.assertEqual(ci_run.recapture_flips(first, second),
                         (["mixed 1h", "value 1h"], ["late 1h"]))

    # explore-unwrap-missing-label at 15m on #1639: the query answers no series on every side; the base and Loki were
    # captured while their query was still loading (run button spinner, blank panel), the PR once it said "No data".
    LOADING_ROW = {"page": "explore-unwrap-missing-label", "range": "15m", "main_pr_diffs": [], "settled": True,
                   "loki_compared": True, "ui_main": {"noData": 0}, "ui_pr": {"noData": 1}, "ui_loki": {"noData": 0}}

    def test_a_resource_regression_alone_does_not_ask_for_a_second_look_or_use_the_cap(self):
        # The first pass keeps it (keep_runtime_observations), so loading the page again clears nothing; the eight
        # mixed-parser rows of #1715 must not use the recapture cap that the rows which can flip need.
        row = {"page": "explore-mixed-parser-explicit-count", "range": "24h", "main_pr_diffs": [], "settled": True,
               "loki_compared": False, "runtime_problems": ["runtime regression: Grafana settlement 16568 -> 24590 ms"]}
        self.assertFalse(ci_run.needs_second_look(row))
        # A proof gap, a difference or an unsettled page still gets one.
        self.assertTrue(ci_run.needs_second_look({**row, "runtime_problems": row["runtime_problems"] + ["runtime proof: required Loki reference lacks a corresponding API answer"]}))
        self.assertTrue(ci_run.needs_second_look({**row, "runtime_problems": ["runtime proof: x"]}))
        self.assertTrue(ci_run.needs_second_look({**row, "main_pr_diffs": ["query: request only on one side (left)"]}))
        self.assertTrue(ci_run.needs_second_look({**row, "settled": False}))

    def test_a_no_data_panel_only_on_the_pr_with_the_same_data_gets_a_second_look(self):
        row = self.LOADING_ROW
        self.assertTrue(ci_run.needs_second_look(row))
        # Every side settled with the same "No data" panel, or Loki shows it too: nothing to load again.
        self.assertFalse(ci_run.needs_second_look({**row, "ui_main": {"noData": 1}}))
        self.assertFalse(ci_run.needs_second_look({**row, "ui_loki": {"noData": 1}}))
        # Beyond the history Loki holds its "No data" may only mean missing history: the base decides alone.
        self.assertTrue(ci_run.needs_second_look({**row, "loki_compared": False, "ui_loki": {"noData": 1}}))
        self.assertFalse(ci_run.needs_second_look({**row, "ui_pr": {"noData": 0}}))
        self.assertFalse(ci_run.needs_second_look({**row, "range": "live"}))
        # The earlier reasons stay.
        self.assertTrue(ci_run.needs_second_look({**row, "ui_pr": {}, "main_pr_diffs": ["x"]}))
        self.assertTrue(ci_run.needs_second_look({**row, "ui_pr": {}, "settled": False}))

    def test_a_no_data_panel_only_on_the_pr_is_loaded_again_more_patiently(self):
        calls = []
        with tempfile.TemporaryDirectory() as out:
            with open(os.path.join(out, "compare.json"), "w", encoding="utf-8") as f:
                json.dump([self.LOADING_ROW, {**self.LOADING_ROW, "page": "same", "ui_pr": {"noData": 0}}], f)
            plan = {"entries": {"explore-unwrap-missing-label": {"kind": "explore", "ranges": ["15m"]},
                                "same": {"kind": "explore", "ranges": ["15m"]}}}
            a = argparse.Namespace(max_recapture=6, shard=None, workers=1)
            meta = {}
            saved = ci_run.run
            try:
                def finish(cmd, **kw):
                    env = kw.get("env", {})
                    calls.append((cmd, env))
                    self.assertNotEqual(env["VP_OUT"], out)
                    for tree, ext in (("data", "json"), ("shots", "png")):
                        dest = Path(env["VP_OUT"], tree, "explore-unwrap-missing-label", "15m")
                        dest.mkdir(parents=True)
                        for side in ("main", "pr", "loki"):
                            (dest / f"{side}.{ext}").write_text("{}")
                    return SimpleNamespace(returncode=0)
                ci_run.run = finish
                self.assertTrue(ci_run.recapture(a, out, plan, 1, 1, meta))
            finally:
                ci_run.run = saved
            with open(os.path.join(out, "plan-recapture.json"), encoding="utf-8") as f:
                self.assertEqual(json.load(f), {"entries": {"explore-unwrap-missing-label": {"kind": "explore", "ranges": ["15m"]}}})
        self.assertEqual(meta["recaptured"], ["explore-unwrap-missing-label 15m"])
        self.assertEqual(meta["recapture_candidates"], 1)
        (cmd, env), = calls
        self.assertEqual(cmd, ["npx", "playwright", "test"])
        self.assertEqual((env["VP_QUIET_MS"], env["VP_SETTLE_MS"]), ("10000", "150000"))

    def test_partial_retry_cannot_combine_new_main_with_stale_pr_and_loki(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as out:
            page = self.LOADING_ROW["page"]
            rng = "15m"
            (Path(out) / "compare.json").write_text(json.dumps([self.LOADING_ROW]))
            for tree, ext in (("data", "json"), ("shots", "png")):
                dest = Path(out, tree, page, rng)
                dest.mkdir(parents=True)
                for side in ("main", "pr", "loki"):
                    (dest / f"{side}.{ext}").write_text("first")
            def partial(cmd, env, **kw):
                dest = Path(env["VP_OUT"], "data", page, rng)
                dest.mkdir(parents=True, exist_ok=True)
                (dest / "main.json").write_text("retry")
                return SimpleNamespace(returncode=0)
            meta = {}
            with patch.object(ci_run, "run", partial), self.assertRaisesRegex(RuntimeError, "incomplete"):
                ci_run.recapture(SimpleNamespace(max_recapture=6, shard=None, workers=1), out,
                                 {"entries": {page: {"kind": "explore", "ranges": [rng]}}}, 1, 1, meta)
            self.assertFalse(meta.get("recaptured"))
            self.assertEqual(Path(out, "data", page, rng, "main.json").read_text(), "first")
            self.assertEqual(Path(out, "first-pass/data", page, rng, "pr.json").read_text(), "first")

    def test_the_recapture_runs_before_the_live_tail_pass_and_one_compare_sees_both(self):
        # The live generator writes into the streams the static windows read, and Loki's index/stats is an estimate from
        # chunk metadata that moves while it writes: a recapture after it compares a different Loki answer.
        calls = []
        with tempfile.TemporaryDirectory() as out:
            plan_path = os.path.join(out, "in-plan.json")
            with open(plan_path, "w", encoding="utf-8") as f:
                json.dump({"run": True, "captures": 2, "entries": {"p": {"kind": "explore", "ranges": ["1h"]},
                                                                   "t": {"kind": "tail", "ranges": ["live"]}}}, f)
            with open(os.path.join(out, "state.json"), "w", encoding="utf-8") as f:
                json.dump({"end": 1, "ports": {"grafana": 1, "loki": 2}}, f)

            def step(name, result=None):
                def fn(*args, **kw):
                    calls.append(name)
                    return result
                return fn
            patches = {"git": lambda *a: "sha", "capture": step("capture"), "recapture": step("recapture", True),
                       "capture_live": step("capture_live", True),
                       "run": lambda cmd, **kw: calls.append("compare" if cmd[1].endswith("compare.py") else
                                                             "montage" if cmd[1].endswith("montage.py") else "stack")}
            saved = {k: getattr(ci_run, k) for k in patches}
            argv = sys.argv
            try:
                for k, fn in patches.items():
                    setattr(ci_run, k, fn)
                sys.argv = ["ci_run.py", "--base", "main", "--plan", plan_path, "--out", out]
                self.assertEqual(ci_run.main(), 0)
            finally:
                sys.argv = argv
                for k, fn in saved.items():
                    setattr(ci_run, k, fn)
        steps = [c for c in calls if c != "stack"]
        self.assertEqual(steps, ["capture", "compare", "recapture", "capture_live", "compare", "montage"])


if __name__ == "__main__":
    unittest.main()
