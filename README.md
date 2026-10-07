# h2hdb-ingest

`h2hdb-ingest` 將 Hentai@Home 下載的作品整理成 H2HDB 目錄，並可產生供
Komga 與 OPDS 閱讀器使用的 CBZ 和縮圖。它會持續觀察下載目錄，處理新增、
變更與刪除的作品，保留原始下載檔案作為來源。

這個服務負責整理與發布書庫；瀏覽和閱讀請搭配 Komga 或
[`h2hdb-opds`](https://github.com/Kuan-Lun/h2hdb-opds)。
若只需要目錄資料，也能關閉 CBZ 輸出。

## 使用前準備

- Python 3.14 以上。
- 已有下載內容的目錄；每個完成的作品包含圖片與 `galleryinfo.txt`。
  可使用多層分類目錄。
- 已初始化的 SQLite 或 MariaDB 資料庫。本 checkout 的相依範圍是
  `h2hdb>=0.43.0,<0.46.0`，使用 schema epoch 3、version 9。
- 啟用 CBZ 時，另備可寫入的書庫目錄，以及處理圖片、暫存工作和待發布
  CBZ／縮圖所需的空間。下載目錄與書庫目錄不得相同或互相包含。

書庫由 ingest 單獨寫入，閱讀器只能唯讀存取。請勿手動改名或修改 ingest
產生的檔案，也不要讓其他程式管理同一個輸出目錄。

## 安裝

在這份 checkout 的根目錄執行以下 Bash／Zsh 指令：

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install .
h2hdb-ingest --help
```

安裝程式會一起解析相容的 H2HDB 與影像處理相依套件。下列指令皆在已啟用
的環境執行；請將 `/data/...` 換成服務帳號或容器內實際可見的路徑。

## 建立資料庫

若已有相容的資料庫，可沿用它並跳到書庫設定。新 SQLite 資料庫的設定
存成 `core.json`：

```json
{
  "database": {
    "sql_type": "sqlite",
    "database": "/data/h2hdb/catalog.sqlite"
  }
}
```

建立父目錄，再初始化與檢查：

```bash
mkdir -p /data/h2hdb
python -m h2hdb migrate --config core.json
python -m h2hdb check --config core.json
```

`migrate` 適用於新資料庫或續做同一次初始化，不會升級任意舊資料庫。
Ingest 本身不建立或升級 schema。

MariaDB 使用 `"sql_type": "mariadb"`，並設定 `host`、`port`、`user`、
`password` 和 `database`；先建立資料庫及具備所需寫入權限的帳號。
完整管理方式見 [H2HDB 說明](https://github.com/Kuan-Lun/h2hdb#readme)。

## 準備書庫目錄

需要 CBZ 與縮圖時，先建立以下目錄：

```bash
mkdir -p /data/h2hdb/library/current/acquisitions
mkdir -p /data/h2hdb/library/current/artwork
mkdir -p /data/h2hdb/library/.h2hdb-coordination
```

這些目錄必須是真實目錄而非符號連結，位於相同檔案系統，並允許 ingest
帳號寫入。容器部署須先在主機建立，再建立閱讀器容器。Ingest 會自行建立
私有的 `.h2hdb-state`，請勿手動建立或修改其內容。

| 服務 | 掛載的書庫子目錄 | 權限 |
| --- | --- | --- |
| ingest | 整個 `library/` | 讀寫 |
| Komga | `library/current/acquisitions/` | 唯讀 |
| OPDS | `library/current/` | 唯讀 |
| OPDS | `library/.h2hdb-coordination/` | 唯讀 |

Komga 只應讀取 `acquisitions/`，避免將 `artwork/` 的縮圖當成書籍。
`.h2hdb-state` 只供 ingest 使用。只發布目錄資料時可略過本步驟。

## 設定 ingest

將下列內容存成 `ingest.json`：

```json
{
  "core": {
    "database": {
      "sql_type": "sqlite",
      "database": "/data/h2hdb/catalog.sqlite"
    }
  },
  "paths": {
    "download_path": "/data/hath-download",
    "library_path": "/data/h2hdb/library"
  }
}
```

資料庫設定與 `core.json` 相同，但須放在 `core` 內。`download_path` 必須
已存在且非空。將 `library_path` 設成 `null`，即可只發布目錄資料，
不解碼圖片、不產生 CBZ 或縮圖。

通常保留預設值即可。需要調整時，在 `paths` 或頂層 `resident` 加入設定：

| 設定 | 預設值 | 用途 |
| --- | --- | --- |
| `paths.max_image_short_side` | `768` | 輸出頁面的短邊上限，接受 1–8192 像素；維持比例且不放大圖片。 |
| `paths.page_render_workers` | `null` | 自動選擇圖片工作數，最多 16；可明確設為 1–16，記憶體吃緊時降低。 |
| `resident.publication_batch_galleries` | `null` | 每輪納入全部符合條件的完整作品；設為 1–1,000,000 可限制每輪新納入作品數。 |
| `resident.progress_log_interval_seconds` | `60` | 工作中的進度摘要間隔，單位為秒。 |
| `resident.source_quiet_seconds` | `300` | 觀察到變更後，等待來源安靜這麼多秒再同步。 |
| `resident.source_max_wait_seconds` | `1800` | 即使持續變動，最遲等待這麼多秒便同步；不得小於安靜間隔。 |
| `resident.source_probe_interval_seconds` | `30` | 背景來源檢查完成後，到下一次檢查的間隔。 |

`paths.render_policy` 可設定 `page_jpeg_quality`（預設 90）、
`thumbnail_jpeg_quality`（85）、`optimize`（`true`）和
`resampler`（`"lanczos"`）。JPEG 品質接受 0–95；重採樣另支援
`nearest`、`box`、`bilinear`、`hamming`、`bicubic`。
變更圖片設定可能需要重新檢查來源和產生輸出。

完整字串 `${ENV_NAME}` 可讀取環境變數，例如
`"password": "${H2HDB_PASSWORD}"`；不支援 `"db-${INSTANCE}"` 這類
字串內插。缺少環境變數或不明設定欄位都會使啟動失敗。

## 啟動與停止

持續處理既有下載並觀察後續變更：

```bash
h2hdb-ingest --config ingest.json
```

只嘗試完成一輪發布：

```bash
h2hdb-ingest --config ingest.json --once
```

單次執行仍遵守作品數限制；不完整或仍在變動的作品留待下次。
若因工作租約衝突或空間不足而未完成發布，指令會失敗。
中斷後若先續做舊的一輪，新抵達的作品須由下一次執行處理。

若要在已初始化、尚無發布的資料庫建立第一個非空目錄：

```bash
h2hdb-ingest-bootstrap --config ingest.json
```

Bootstrap 遇到已有發布的資料庫會拒絕執行；完成首次非空發布後便結束。
後續請啟動常駐模式。也可用
`python -m h2hdb_ingest --config ingest.json` 啟動同一個常駐服務。

以 `Ctrl+C` 或 `SIGTERM` 正常停止。服務會完成目前的小步驟及資源清理後
離開；已開始的完整資料庫稽核可能使停止需要較長時間。

## 何時能在閱讀器看到作品

第一次執行預設先處理全部符合條件的完整作品，再分析與發布。
閱讀器要等整輪發布完成才會看到結果，單一 CBZ 渲染完成不代表已發布。
如果希望分批看到成果，可設定 `resident.publication_batch_galleries`；
例如 `100` 限制每輪新增作品數，但不限制書庫總數、處理時間或磁碟用量。
大量作品以小批次處理時，重複分析整個目錄可能增加總處理時間。

請先完成圖片寫入，再寫入 `galleryinfo.txt` 作為完成標記。來源檔案應在
整輪工作期間保持可讀；變動或未完成的作品會延後。已完成作品的目錄是
掃描終點，其中再嵌套的作品不會被發現。暫時移除完成標記會保留上次發布
內容；確認整個作品目錄已刪除後，才會從來源集合移除。

啟用圖片輸出時：

- 支援 `.avif`、`.bmp`、`.gif`、`.jpeg`、`.jpg`、`.png`、`.webp`，
  副檔名不分 ASCII 大小寫；其他一般檔案不會渲染為頁面。
- 每頁轉成 JPEG，GIF 只取第一影格。任一頁無法解碼時會拒絕整本，
  日誌會指出原因；修好來源並更新 `galleryinfo.txt` 後可重新處理。
- 輸出為 `h2h-<gid>.cbz`，包含 metadata 和排序後的頁面；第一頁作為封面，
  另產生最長邊 320 像素的縮圖。沒有合格頁面的作品只有 metadata CBZ。
- 每本最多 4096 頁、每個輸出 JPEG 最多 32 MiB、長邊最多 8192 像素、
  每頁最多 40 MP，CBZ 最多 2,147,483,647 bytes。

大圖會縮小且不放大；部分影像格式仍可能需要大量記憶體，工作數上限不是
記憶體硬上限。去重和內容篩選會考慮整個已知集合，因此新增作品也可能
替換或移除舊的發布內容，書籍數不一定每輪增加。

## 日常維護與問題排查

INFO 日誌會顯示啟動檢查、目前階段、進度及發布結果。空閒時沒有固定
進度訊息。需要更多診斷時，在 `core` 內加入
`"logger": {"level": "DEBUG"}`。進度是累計快照；不同階段的時間可能
重疊，不應直接相加。發布完成、清理完成與下一輪取得工作是不同階段。

保留足夠空間供圖片處理、單本來源暫存、資料庫計畫，以及整輪所有待發布
輸出使用。空間不足時工作保留待重試；釋放空間或調整配額後讓服務重試，
不要刪除私有 journal、staging 或 coordination 檔案。

意外中斷後，以相同資料庫和完整書庫重新啟動。服務會續做發布與清理；
未完成時閱讀器可能暫時無法使用。請勿手動刪除 `ACTIVATING` 或鎖檔。
不明檔案、內容不符或符號連結會被保留並回報，供檢查處理。

服務會定期稽核資料庫；首次啟動、上次未正常停止或稽核到期時可能執行
完整檢查。也可手動執行 `python -m h2hdb check --config core.json`。

| 現象 | 處理方式 |
| --- | --- |
| `download_path is empty` | 檢查下載路徑及容器掛載是否正確。 |
| `must be a pre-existing real directory` | 建立必要的書庫目錄，並確認它們不是符號連結。 |
| 資料庫不是 `READY` | 新資料庫先初始化；既有資料庫先查看版本或稽核錯誤。 |
| 圖片被拒絕 | 依日誌修復來源圖片，再更新完成標記。 |
| 空間不足或配額錯誤 | 檢查回報的檔案系統，保留尚未完成的私有狀態。 |
| `library relocation is unfinished` | 維持服務停止，在同一目的地重新執行搬移驗證。 |
| 書庫 identity 改變 | 檢查掛載是否被換掉；刻意搬移完整書庫時使用下述搬移指令。 |

## 升級與搬移

離線維護前，停止 ingest、OPDS、Komga 和其他使用書庫的程序，備份資料庫
與完整書庫。套件須一起符合各自宣告的相依範圍；目前使用 schema 9 與
library journal v5。已符合這兩個格式的資料可繼續使用，無須重建 CBZ。

舊安裝須先確認資料格式，再依對應歷史 checkout 的說明及匹配環境處理：

| 既有格式 | 處理入口 |
| --- | --- |
| exact journal v4 | Ingest 0.28.0 的 `upgrade-library-journal-v4-to-v5.py`；保留書庫 UUID、發布狀態、CBZ 和縮圖。 |
| Core schema 8 或其中斷的轉換 | Core 0.43.0 的 `scripts/upgrade-observation-upload-time-schema.py`，轉到 schema 9。 |
| Core schema 7 | 先用 Core 0.41.2 的來源集合轉換工具到 schema 8，再處理下一步。 |
| Core schema 6 | 先用 Core 0.40.0 的稽核轉換工具到 schema 7，再逐步處理。 |

目前 checkout 不含上述一次性轉換工具，正常啟動也不會自動轉換。
轉換期間保留原始下載和完整備份；Core 轉換與書庫 journal 轉換是不同步驟。
其他更舊的 Core schema，以及包含 `current/hash-v1`、
`.h2hdb-state/coordination` 或 journal v1–v3 的舊書庫，須保留原檔，
另建新資料庫與新書庫，從原始下載重新整理。搬移指令不負責升級舊格式。

搬移目前格式的書庫到新路徑或檔案系統：

1. 停止 ingest、閱讀器及任何可能修改書庫的程序。
2. 搬移整個書庫，包含 `.h2hdb-state` 和 `.h2hdb-coordination`，沿用原資料庫。
3. 更新所有讀寫服務的路徑或掛載。
4. 執行驗證：

   ```bash
   h2hdb-ingest-relocate --library /new/location/library
   ```

5. 指令回報完成後，再啟動 ingest 和閱讀器。

中斷時以相同目的地重跑即可續做。此操作保留書庫 identity、目錄與檔案
內容；僅複製 CBZ 到另一個空書庫並不能保留原有資料庫綁定。

## 進一步資訊

- [可靠性與驗證範圍](verification/README.md)：故障恢復、搬移與模型證據。
- [設定定義](src/h2hdb_ingest/config.py)：完整欄位、預設值與限制。
- [問題回報](https://github.com/Kuan-Lun/h2hdb-ingest/issues)：請提供套件版本、
  資料庫種類、執行指令與錯誤訊息，移除帳密和私有路徑。

## 授權

GPL-3.0-only，詳見 [LICENSE](LICENSE)。
