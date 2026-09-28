# Databricks メトリクスビュー もくもく会

JEDAI (Japan Enduser Group | Databricks Innovation) のもくもく会で使う教材です。Unity Catalog のメトリクスビューを、Databricks Free Edition で実際に作って触ってみます。

メトリクスビューは、集計した表ではなく、指標の計算方法を登録しておくビューです。数字そのものは持たず、どの単位で計算するかは聞く人が決めます。この1行を手で確かめるための教材です。

- イベント: [Databricks メトリクスビュー はじめてのもくもく会](https://jedai.connpass.com/event/407004/) (2026年10月21日 18:30〜20:00 オンライン)
- 説明記事: [集計済みのビューを配るのをやめる。Databricksメトリクスビューを触ってみた \#SQL \- Qiita](https://qiita.com/taka_yayoi/items/bcb8f5729e162d851e79)

## ファイル

| ファイル | 用途 |
|---|---|
| `metric_view_mokumoku.py` | 参加者用。もくもくタイムはこちらを使います |
| `metric_view_demo.py` | 講師が実演で使ったもの。当日の画面をそのまま再現できます |

どちらも Databricks ノートブックのソース形式です。ワークスペースにインポートするとノートブックとして開きます。

## 必要なもの

- Databricks のワークスペース。[Free Edition](https://docs.databricks.com/aws/ja/getting-started/free-edition) で動きます
- サーバレスコンピュート。動作確認は Free Edition のサーバレスで行っています
- メトリクスビューの作成や `MEASURE()` でエラーが出る場合は、ノートブック右上のコンピュートを SQL ウェアハウスに切り替えてください

Unity Catalog が有効なワークスペースであれば、Free Edition 以外でも動きます。その場合はノートブック冒頭のウィジェットで、書き込み権限のあるカタログとスキーマを指定してください。

## 取り込み方

### Git フォルダとして取り込む

ワークスペースで新規に Git フォルダを作成し、リポジトリのURL `https://github.com/taka-yayoi/metric_view_mokumoku` を指定します。2つのノートブックがまとめて入ります。

### ファイル単位で取り込む

GitHub 上でファイルを開き、Raw 表示のURLをコピーします。ワークスペースのインポート画面でURLを貼り付けると、ノートブックとして取り込まれます。

参考: [ノートブックのインポート](https://docs.databricks.com/aws/ja/notebooks/notebook-export-import)

## 参加者用ノートブックの中身

上から順に実行していきます。ほとんどのセルは完成しているので、実行するだけです。自分で書くのは Part 4 の課題3つだけで、解答は各課題のすぐ下に折りたたんで置いてあります。

| Part | 内容 |
|---|---|
| Part 0〜1 | 作業場所の準備と、題材データの作成 |
| Part 2 | 集計した表の落とし穴を確かめる |
| Part 3 | メトリクスビューを作る |
| Part 4 | 切り口を変えて聞く (課題3つ) |
| Part 5 | AI/BI ダッシュボードと Genie から使ってみる |
| おまけ Part 6 | メジャーを1つ足してみる、ほかにもできることの読み物 |

Part 2 が今日の起点です。日ごとに集計した表を足し上げて月の購入顧客数を出すと335人、注文データから直接数えると246人になります。同じ3月のデータなのに値が食い違う理由を確かめてから、Part 3 でメトリクスビューを作ります。

### 題材データ

架空のECショップ「もくもく商店」の注文データです。ノートブックの中で生成します。

- `orders`: 6,000件の注文。2025年1月1日から2026年6月30日まで
- `customers`: 顧客マスタ。都道府県と会員ランクを持ちます

乱数シードを固定してあるので、誰が実行しても同じ数字になります。当日の実演で出てきた数字と、手元の数字が一致します。

## 後片付け

各ノートブックの末尾に、作成したスキーマを削除するセルがあります。コメントを外して実行してください。既定の作業場所は次のとおりです。

- 参加者用: `workspace.jedai_metric_view`
- 講師用: `workspace.jedai_metric_view_demo`

## 参考ドキュメント

- [Unity Catalog のメトリクスビュー](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/)
- [メトリクスビューの作成](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/create)
- [メトリクスビューのクエリー](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/query)
- [メトリクスビュー YAML 構文リファレンス](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/yaml-reference)
- [基本的なモデリング](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/basic-modeling)
- [高度なテクニック](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/advanced-techniques)
- [マテリアライズ](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/materialization)
- [パラメーターの使用](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/use-parameters)
- [Genie オントロジー](https://docs.databricks.com/aws/ja/genie/genie-ontology)
- [Unity Catalog セマンティクス](https://docs.databricks.com/aws/ja/uc-semantics/)
