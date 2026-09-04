# AGENTS.md

## 政策來源

- 本檔是此 repository 的唯一代理開發政策來源。
- 其他代理入口只能要求完整閱讀本檔，不得複製另一份政策。
- 可執行規則以 repository 內的 scripts 與設定檔為準。

## 溝通

- 最終回覆一律使用繁體中文。
- 程式碼、識別字、命令、檔名與 commit message 可使用英文。
- 不得為了承載回覆而新增 Markdown 文件。
- 移除 compatibility path、改變公開行為或採用例外時，必須在對話及
  最終回覆中明確說明。

## 設計與修改原則

- 不預設存在最小修改或向後相容要求。
- 在任務範圍內選擇架構、可讀性與可測試性最好的完整結果。
- 綜合考慮 SOLID、KISS、YAGNI、內聚性與低耦合。
- 必要的局部重構可直接納入任務。
- 若會實質擴大任務範圍、改變原要求未涵蓋的公開行為，或引入資料遷移，
  必須先取得使用者同意。
- 任務直接涉及的 legacy compatibility code 應移除，不保留 shim；不全面
  清理與任務無關的 legacy code。
- generated output 不得直接修改；必須修改 generator 或 source 後重新產生。

## 工作樹與 Git

- 唯讀分析不建立 branch。
- 凡會修改 tracked files 的任務，使用
  `scripts/detect-primary-branch.sh` 判定 primary，並建立專用 task branch。
- 不得 stash、reset、clean、覆寫或混入既有使用者修改。
- 工作樹不乾淨時，從 committed primary 建立獨立 worktree。
- task branch 可包含多個邏輯 Conventional Commits。避免巨大 commit；小而
  內聚的任務仍可只有一個 commit。
- 任務完成後執行 `scripts/git-flow-merge.sh`。該腳本負責完整 gate、
  `--no-ff` merge、安全移除 task worktree，以及以 `git branch -d`
  刪除已合併的本機 branch。
- primary 在任務期間可以推進；整合只要求 primary 與 task branch 有共同
  ancestor，不要求 task branch 仍直接基於目前 primary tip。
- merge conflict 或 gate failure 時必須 abort merge 並保留 task branch。
- merge 後收到的任何 follow-up 都建立新的 task branch。
- 本機 task branch、commit、`--no-ff` merge 與 `branch -d` 已獲預先
  授權。
- fetch、pull、push、remote branch、tag、release、publish、deploy 與任何
  force 操作仍須逐次明確授權。
- 不得使用 `--no-verify`。

## 提交格式

- 所有非 merge commit 必須符合 Conventional Commits。
- Breaking change 使用 `type!:` 或 `BREAKING CHANGE:` footer。
- project version 更新使用獨立 commit：
  `chore(release): bump version to X.Y.Z`。

## 版本政策

- `pyproject.toml` 的 `[project].version` 是唯一 project version source。
- project version 固定使用 `X.Y.Z`。
- 1.0 前，`Y` 是 compatibility lane，`Z` 是同一 lane 內的相容 release
  counter。相容修正或功能遞增 `Z`；breaking change 遞增 `Y` 並將
  `Z` 歸零。
- 1.0 後使用標準 Semantic Versioning。
- 整個 task branch 只在整合前更新一次 project version。
- shipped runtime 或 deployment surface 有變更時，至少需要相容升版。
- Breaking API、CLI、config、schema、protocol、資料格式或 Python/platform
  support 變更必須提高 compatibility lane 或 major。
- tests、一般文件、IDE、hooks、CI 與 dev-only tooling 單獨變更時不升版。
- 未分類路徑必須明確判定 impact，不得靜默當作 `none`。
- `Version-Impact: none` 必須附具體理由，並在最終回覆揭露。
- project version 變更必須觸發完整 direct dependency audit。
- `scripts/check-version.py` 以 staged merge candidate 判定 release surface，
  並強制 task-level `X.Y.Z` 只升版一次；pre-merge gate 不得略過。
- 升版後執行 `scripts/audit-dependencies.py --review-note "<相容性結論>"`，
  並將 `.release/dependency-audit.json` 納入 task branch。receipt 必須符合
  candidate version 與完整 dependency manifest。

## 依賴與環境

- repository 必須能從單一乾淨 checkout 重建，不得依賴固定 sibling clone
  路徑。
- 明確跨 repository 任務可使用傳入的 wheel、Git URL/ref 或 repository
  path；sibling discovery 只能是選擇性的效能優化。
- Python registry dependencies 原則上使用 `>=` lower bound；合理 upper
  bound 與 `!=` 可以保留，但必須有相容性依據。
- 精確版本只允許經驗證且有文件理由的特殊契約。
- dependency audit 必須涵蓋 build、runtime、optional 與 development direct
  dependencies，並搜尋現有 upper bound 之外的候選版本。
- 有新版時必須檢查 release notes、驗證相容性並嘗試修正問題。
- `uv.lock` 不得成為環境重建或驗證的輸入；`scripts/rebuild-env.sh` 可
  使用 `uv venv` 與 `uv pip`，但不得使用會依賴 project lockfile 的
  同步流程。
- Node tooling 使用 `npm install --package-lock=false`，不得產生或提交
  `package-lock.json`。
- 不得依賴 system-wide lint、format、type-check 或 Markdown 工具。
- `requires-python` 使用 `>=3.14`；只有經驗證的壞版本可使用 `!=`。

## 品質工具

- `pyproject.toml` 是 Ruff 與 mypy 的唯一規則來源。
- 使用 Ruff lint 與 Ruff formatter，不使用 Black。
- Ruff 使用適合專案的嚴格規則集，不從 `ALL` 出發；每個停用規則必須
  記錄理由。
- mypy 使用標準 `strict = true`。不得保留 `mypy.ini`。
- module 例外使用精確 TOML overrides。
- `type: ignore` 必須指定 error code 並附理由。
- `noqa` 必須指定 rule code 並附理由。
- Markdown 使用 repository-local `markdownlint-cli2`。
- VS Code 使用相同設定與 repository-local environment；CLI gate 是最終
  權威，IDE diagnostics 為即時輔助。

## 檢查分層

- `scripts/format.sh`：明確執行會修改檔案的 formatter 或 fixer。
- `scripts/check-fast.sh`：離線、唯讀的 Ruff、format check、mypy 與
  markdownlint；每次非 merge commit 執行。
- `scripts/run-pytest.py merge`：以 auto-xdist 執行 `not deep`，包含 collection、
  execution、teardown 與 process-group cleanup 的總時間上限為 300 秒；不得啟動
  live MariaDB 或 private corpus。`scripts/check-pytest-deep.sh` 是明確手動、
  不限時的 deep 入口。
- `scripts/check-full.sh`：fast gate、上述 bounded pytest merge profile、build、
  wheel smoke 及本 repository 的特殊檢查；整合候選只跑一次。
- dependency audit 可連網，但 hooks 只驗證本機 receipt，不在 commit
  過程連網。
- GitHub Actions 只呼叫相同 scripts，並保留 trusted publishing、平台特有
  或本機無法可靠重現的檢查。
- 不使用 Claude、Codex 或其他 provider-specific Stop hooks 重複檢查。

## 測試與例外

- runtime 行為變更必須新增或更新測試；bug fix 必須有 regression test。
- 新功能涵蓋正常、邊界與錯誤路徑。
- 數值測試固定隨機種子；容許誤差需有依據。
- flaky test 視為失敗，不得以重跑掩蓋。
- 不設定跨 repository 的統一 coverage 百分比。
- live account、network、production 或 destructive probe 不得進入 hooks、
  一般 pytest 或自動 merge gate。
- `skip` 或 `xfail` 必須有理由；`xfail` 原則上使用 `strict=True`。
- 不得為通過檢查而全域放寬工具設定。

## 完成回報

最終回覆必須包含：

- 實作及公開行為變化。
- 移除的 compatibility path。
- project version 與 dependency audit 結果。
- commits 與完整檢查結果。
- primary branch 與 merge commit。
- branch/worktree 是否已清除。
- 是否仍未 push、publish 或 deploy。

## Repository-specific policy

`h2hdb-ingest` 是 H2HDB 的 filesystem-facing ingest service，發佈名稱為
`h2hdb-ingest`，公開 import package 為 `h2hdb_ingest`。維持
`src/h2hdb_ingest/` layout。

### Ownership boundary

本 repository 擁有：

- deterministic、keyset-paged filesystem discovery 與
  `galleryinfo.txt` parsing；
- exact source-byte observations與 PAGE/OTHER eligibility classification；
- deterministic JPEG/GIF-frame-zero rendering、canonical CBZ writer/parser、
  page byte-extent與 thumbnail-320 evidence；
- opaque filesystem storage-key issuance、artifact protection與 multi-resource
  activation；
- crash-safe Komga current-view reconciliation；
- resident ingest loop 與 ingest-lease heartbeat orchestration。

`h2hdb` core 獨占 connectors、transactions、schema/epoch administration、
durable queues、token-fenced coordination、source checkpoints、
analysis/deduplication policy、catalog repositories、artifact selection 與
publication。只能依賴公開 vNext facade、protocol 與 domain receipt；不得
import core repository、connector internals，或重建已移除的 `H2HDB`
compatibility surface。startup 只能呼叫 `VNextDatabaseAdminFacade.check()`，
不得初始化或 migrate core schema。

Core 只能封存 neutral ordered source member、role、position、hash/size與
adapter回傳的 opaque locator/evidence；不得擁有 CBZ/ZIP member name、
compression、timestamp、Pillow、image eligibility、thumbnail rendering policy或
filesystem layout語意。Core 可以擁有 generic thumbnail destination與封存
neutral descriptor，但不得決定其格式、尺寸、品質、storage key或 bytes。Ingest
filesystem observation負責分類 PAGE/OTHER；canonical writer
對每個 PAGE 都必須輸出一個 dense JPEG page，decode失敗則整個artifact fail。

每個 core operation 維持 issue/prepare/commit 分離。session controller lock
只包住有界的 database issue 或 commit call；filesystem scan、hash、image/ZIP
work、artifact staging、activation spooling 與其他 local I/O 必須在 lock
之外，讓 heartbeat 能更新精確 session receipt。不得提供 registry surrogate
ID；建立 immutable natural `VNextIngestPolicy` facts，由 core 配置 authority。

### Single-library activation safety

- CBZ-enabled deployment 只有一個 `library_path` parent。`current/acquisitions/`
  是唯一 persistent CBZ tree；`current/artwork/` 是 standalone thumbnail
  tree；`.h2hdb-coordination/` 是 reader-visible publication fencing namespace；
  `.h2hdb-state/` 只擁有 private staging、quarantine、journal 與 locks。
- `library_path` 是 deployment 預先建立的真實 bind-mount root；runtime 不得建立
  mount root 或依賴容器內不可見的 host parent fsync。Compose 擁有 service identity、
  mount scope 與 read-only/read-write policy；runtime 不得把 host UID、GID 或 POSIX
  mode 當成資料正確性條件，也不得 `chmod` 或 `chown` 既有或新建 entry。建立 API
  可以傳入安全的初始 mode，但它受 umask 與 host filesystem policy 約束。每個
  managed directory 與 persistent control file 建立或 replay 時仍必須 child fsync、
  parent fsync，再重驗 real type、exact dev/inode identity 與必要的 link authority。
- 所有 CBZ-enabled deployment 在建立 consumers 前都必須預建立空的 `current/`、
  `current/acquisitions/`、`current/artwork/`與 `.h2hdb-coordination/` reader
  bind sources。Runtime 必須 idempotently durable revalidate這四個 roots的 type
  與 identity，不得建立或修改其 metadata；
  `.h2hdb-state/` 及其 private descendants 仍只由 ingest 建立。Host ACL或mode必須
  讓 Compose指定的 ingest identity實際完成所需I/O，但 runtime不規定其具體值。
- Legacy `.h2hdb-state/coordination` entry 無論為 directory、symlink 或
  其他類型都必須在修改 private state 前 fail closed；不得 migrate、fallback
  或接納舊 coordination layout。
- Legacy `current/hash-v1`與 activation journal format v1/v2必須明確 fail
  closed並要求 fresh rebuild；不得自動刪除、migrate或和v3 journal混合啟動。
- `library_path` parent、`.h2hdb-state/` 與 `.h2hdb-coordination/` 是
  ingest-owned single-writer namespace；其他程序即使使用相同 UID 也不得
  mutate。Komga只能 read-only mount `current/acquisitions/`；OPDS read-only mount
  `current/`與 `.h2hdb-coordination/`。不得把含 artwork的整個 `current/`交給
  Komga；public `current/` unknown-entry race仍須 preserve bytes並 fail closed。
- filesystem path只能使用 ingest adapter發出的 opaque `managed-filesystem-v2`
  key。Acquisition為 `acquisitions/hash-v2/<2>/<1>/h2h-<gid>.cbz`；thumbnail為
  `artwork/hash-v2/<2>/<1>/h2h-<gid>/thumbnail-320.jpg`；兩者 shard皆為
  `sha256(b"h2hdb-storage-object-shard-v2\0" + u64be(gid))`。Constructor只在
  ingest，core不得重算或解讀segments。Activation必須驗codec、canonical path、
  GID與resource kind完全一致；不得提供舊key shim、日期 grouping、friendly title
  path或第二份 persistent CBZ tree。
- canonical archive必須是 closed-world `galleryinfo.txt`加dense
  `pages/{page_index:04d}.jpg`。Metadata固定DEFLATE；PAGE固定ZIP_STORED；flags、
  comment、extra、data descriptor與ZIP64皆禁止。最多4096 PAGE、每個source/output
  encoded page 32 MiB、decoded 40 MP、long side 8192、aggregate archive
  2,147,483,647 bytes。Render policy預設page quality 90、thumbnail quality 85、
  optimize true與LANCZOS；所有byte-affecting選項都必須bounded、寫入policy
  fingerprint且由frozen config/domain傳遞。GIF只取frame zero；page zero是
  full-size cover alias。Standalone thumbnail-320不得request-time resize。
  Writer完成後必須重驗central/local header、CRC、SHA-256、size與page
  extents，destination partial write必須fail closed。
- acquisition與thumbnail artifact必須先在同 filesystem 的 private staging以
  exact-prefix resumable temp完整寫入、驗證 SHA-256/size並fsync。activation先以
  atomic
  no-replace capture 舊 current，再以 descriptor-relative、逐層
  `O_NOFOLLOW` 的 atomic no-replace rename 把 stage 移入 current；不得
  hard-link 或 byte-copy persistent resource。rename 成功與 response-loss replay
  都必須 fsync current parent 及 staging directory。temp publish 到 stage 或
  marker 的 response-loss replay 也必須重新 fsync owning directory，才能推進
  journal state。若保守的 power-loss recovery 同時顯示 rename 兩個名稱，只能
  在 journal facts、digest、size 與 durable dev/inode/mtime authority 完全相符，
  且兩名確為同一個 nlink=2 inode 時，fsync 兩側後移除 source duplicate、再次
  捕獲 post-unlink exact signature、fsync survivor inode 與兩側 directory，再
  重驗 survivor；不同 inode 即使 bytes 相同也必須 preserve 並 fail closed。
- core reader head 在 library durable `READY` 前不得 advance。activation 從
  reader-invisible DB commit 起持有
  `.h2hdb-coordination/publication.lock` exclusive
  flock，建立並 fsync `ACTIVATING`；core finalization 成功後才可 durable unlink
  marker 並 unlock。OPDS 只取得 nonblocking shared flock，marker/lock異常時
  fail closed。
- `reconcile_page` 每次最多處理 128 個依 `(publication_key, resource_kind)`排序的
  install/removal並回寫 opaque cursor。同一GID可同時有ACQUISITION與THUMBNAIL；
  下一revision未引用的resource必須exact stale removal。
  SIGINT/SIGTERM 在 bounded step 間停止且不得再 claim；SIGKILL 後由 exact
  receipt、journal、marker、digest 與 stat identity 繼續，不得要求 rollback。
- 只能 capture/install/delete journal 記錄為 managed 且 exact authority 相符的
  path。unknown path、中間 symlink、name/inode/link/content authority變更一律
  fail closed。唯一 partial-content 例外是 durable `WRITING` token 所綁定的
  deterministic private temp；terminal release 必須先 durable tombstone，再
  捕獲其實際 digest/stat identity 後 descriptor-relative delete。
- terminal `RELEASED` protection token tombstone 永久保留並 fence delayed
  `protect`；tombstone 或 cleanup response loss 必須由保留的 staging authority
  重播。若 replay 已看不到 authorized leaf，必須先 fsync owning directory 並
  再確認 absent，才能清除 journal authority。private cleanup 每次最多推進 one
  eight-item page，不得刪除 current bytes。
- stage token 轉為 `INSTALLED` 並清除 inode authority，必須和
  `current_entries` authority 及 pending activation completion 在同一個 SQLite
  transaction 發生；不得留下兩個 transaction 間的 crash gap。
- resident 在每次 claim 前與 session 完成後各執行一次 library maintenance。
  `PROGRESSED` 立即重試；`BLOCKED`、`CONTENDED`、`DONE` 與 transient
  failure 回到正常 poll cadence。

### Verification

- 一般 tests 必須離線且使用 temporary roots/fakes；private corpus 永遠
  opt-in，不得進入自動 merge gate。
- MariaDB resident-lifecycle E2E 使用 testcontainers 的 MariaDB 10.11.11，
  只有明確設定 `H2HDB_TEST_MARIADB=1` 時執行；release validation 必須跑，
  一般 commit 與 merge gate 不得啟動 MariaDB 或其他 live service。唯一的
  Docker 例外是 host Java 不可用時，以 lockfile digest-pinned、network-off
  container 執行 TLC。
- `scripts/check-full.sh` 執行 bounded 離線 pytest、Lean verification、
  checksum-pinned TLC Small profile、sdist/wheel build 與 installed-wheel
  CLI/import smoke。
- `verification/` 是 executable design model，不代表 runtime 自動符合。
  Lean/TLA 變更仍需 differential test、crash/fault injection 與 implementation
  conformance evidence。
- retained incremental oracle 與 Lean model 只描述 global analysis semantics；
  production analysis authority 仍屬 `h2hdb`，不得在本 repository 建立第二
  份 runtime deduplication implementation。
