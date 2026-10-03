# Pi

Install Pi 1.0.0 or newer on the machine running your T3 environment:

```sh
npm install --global @earendil-works/pi-coding-agent
pi
```

Use Pi's `/login` command, provider API keys, or `models.json` to configure a
model. Then open **Settings > Providers > Add provider**, choose **Pi**, and
enable the instance. The model picker uses Pi's authenticated model catalog;
model identifiers have the form `provider/model`. Refresh provider status after
changing credentials or models. See [Pi's model configuration](https://github.com/earendil-works/pi/blob/v1.0.0/packages/coding-agent/docs/models.md).

Leave **Pi configuration directory** empty to use Pi's normal profile, or set a separate
directory for another account. Instance environment variables can supply API
keys or `PI_CODING_AGENT_DIR`. An explicit configuration directory takes precedence.
Each T3 instance keeps its conversation files separately, so stopping one
instance does not stop the others. Existing Pi sessions outside T3 remain separate.

**Supervised** asks before bash, edits, writes, and other extension tools.
**Auto** uses the same rules. **Auto-accept edits** allows built-in edits
and writes while keeping approvals for commands and other tools. **Full access**
allows tools without T3 approval. Approvals apply once; Pi does not offer T3's
workspace-wide approval grants. Plan mode permits built-in file inspection and
blocks commands, edits, and extension tools.

These are tool approval rules. Pi runs with your operating-system permissions;
T3 does not add an OS sandbox. Trusted Pi extensions and startup hooks run as
part of the Pi process. Use Pi's normal project-trust controls and review extensions
before loading them. See [Pi security](https://github.com/earendil-works/pi/blob/v1.0.0/packages/coding-agent/docs/security.md).

Pi threads support streaming text and thinking, images and file attachments,
model changes, interruption, native compaction, and resuming after a server
restart. Sending while Pi is working interrupts and drains the native run, then
continues the same T3 turn with the new message. Native extension confirmation
and input dialogs appear in T3. T3's MCP tools are supplied to each chat process
without changing Pi's saved MCP configuration. Conversation rewind is unavailable;
start a new thread to discard
native history. Title and Git text generation use a separate process with tools
and extensions disabled.

Usage includes T3's Pi sessions and native Pi sessions in configured profiles,
with cache reads, cache writes, output tokens, and Pi-reported cost. Custom
provider extensions still load in chat sessions. Health checks do not load
extensions; add a `provider/model` custom model in T3 if its provider is defined
only by an extension.
Use a built-in or `models.json` provider for title and Git text generation;
those helpers deliberately do not load provider extensions.

Updates to Pi use the package manager that installed it. T3's Pi adapter is
developed against [Pi v1.0.0](https://github.com/earendil-works/pi/releases/tag/v1.0.0).
For a local conformance check without real credentials or paid model calls:

```sh
cd apps/server
T3_PI_SMOKE_BINARY=pi vp test run src/provider/pi/PiRuntime.smoke.test.ts
```

The smoke tests use temporary profiles, a local model endpoint, and isolated
T3 state. They never use your live T3 database or Pi credentials.
