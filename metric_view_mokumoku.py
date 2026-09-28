# Databricks notebook source
# MAGIC %md
# MAGIC # メトリクスビューを触ってみる
# MAGIC
# MAGIC 架空のECショップ「もくもく商店」のデータを使います。実演でお見せしたものと同じデータ、同じ数字です。
# MAGIC
# MAGIC **メトリクスビューを一言で言うと、集計した表ではなく、指標の計算方法を登録しておくビューです。** 数字そのものは持たず、どの単位で計算するかは聞く人が決めます。このノートブックは、この1行を手で確かめるためのものです。
# MAGIC
# MAGIC 進め方
# MAGIC
# MAGIC - 上から順に実行してください
# MAGIC - ✍️ が付いている行は、予想を書いてから次を実行してください。当たり外れは気にしなくて大丈夫です
# MAGIC - **課題** が付いているセルは、自分で書き換えてから実行します。書き方の例がすぐ上にあります
# MAGIC - 手が止まったら、課題の文中にあるヒントを読んでください。それでも進まなければ、課題のすぐ下に「解答」の折りたたみがあります。クリックすると開くので、遠慮なく開いてください
# MAGIC - 当日はチャットで質問してください
# MAGIC
# MAGIC 今日の必須は **Part 2 と Part 3 と Part 4** です。Part 5 とおまけは、時間が余った方と持ち帰り用です。
# MAGIC
# MAGIC | Part | 内容 | 目安 |
# MAGIC |---|---|---|
# MAGIC | Part 0〜1 | 準備とデータ作成 | 5分 |
# MAGIC | Part 2 | 集計した表の落とし穴を確かめる | 8分 |
# MAGIC | Part 3 | メトリクスビューを作る | 5分 |
# MAGIC | Part 4 | 軸を変えて聞く (課題3つ) | 12分 |
# MAGIC | Part 5・おまけ | ダッシュボード、Genie、メジャーを足す | 残り時間と持ち帰り |
# MAGIC
# MAGIC 環境
# MAGIC
# MAGIC - Databricks Free Edition (サーバレス) で動きます
# MAGIC - メトリクスビューの作成や `MEASURE()` でエラーが出る場合は、右上のコンピュートを SQL ウェアハウスに切り替えてください

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0. 準備
# MAGIC
# MAGIC この後の作業場所になるスキーマを作ります。Free Edition では `workspace` カタログがそのまま使えます。

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "カタログ")
dbutils.widgets.text("schema", "jedai_metric_view", "スキーマ")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"作業場所: {CATALOG}.{SCHEMA}")

# COMMAND ----------

def _workspace_host():
    try:
        return spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        return ctx.browserHostName().get()

WORKSPACE_HOST = _workspace_host()

def show_link(name, label=None):
    url = f"https://{WORKSPACE_HOST}/explore/data/{CATALOG}/{SCHEMA}/{name}"
    displayHTML(f'<a href="{url}" target="_blank">{label or name} をカタログエクスプローラーで開く</a>')

show_link("", f"スキーマ {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1. 題材データを作る
# MAGIC
# MAGIC 「もくもく商店」の2つのテーブルを作ります。
# MAGIC
# MAGIC | テーブル | 中身 | 主な列 |
# MAGIC |---|---|---|
# MAGIC | `orders` | 注文 (1行 = 1注文) | 注文日、顧客ID、商品名、カテゴリ、数量、金額、ステータス |
# MAGIC | `customers` | 顧客 | 顧客ID、都道府県、会員ランク |
# MAGIC
# MAGIC 期間は2025年1月から2026年6月までの18か月、6,000行です。ステータスには「完了」と「キャンセル」があります。

# COMMAND ----------

import random
import datetime

random.seed(42)

PREFS = ["東京都", "神奈川県", "大阪府", "愛知県", "福岡県", "北海道", "宮城県", "広島県"]
RANKS = ["一般", "シルバー", "ゴールド"]

customers = [
    (f"C{i:04d}", random.choice(PREFS), random.choices(RANKS, [6, 3, 1])[0])
    for i in range(1, 501)
]

products = [
    ("P001", "ワイヤレスイヤホン", "家電", 12800),
    ("P002", "電気ケトル", "家電", 5980),
    ("P003", "ロボット掃除機", "家電", 39800),
    ("P004", "ドリップコーヒー詰め合わせ", "食品", 1980),
    ("P005", "国産はちみつ", "食品", 1480),
    ("P006", "おかき詰め合わせ", "食品", 980),
    ("P007", "文庫本セット", "書籍", 2400),
    ("P008", "技術書", "書籍", 3520),
    ("P009", "今治タオル", "日用品", 2200),
    ("P010", "ハンドソープ詰め替え", "日用品", 680),
]
weight = {"家電": 1, "食品": 6, "書籍": 2, "日用品": 4}

start = datetime.date(2025, 1, 1)
days = (datetime.date(2026, 6, 30) - start).days + 1

orders = []
for i in range(1, 6001):
    cust = random.choice(customers)
    prod = random.choices(products, [weight[p[2]] for p in products])[0]
    order_date = start + datetime.timedelta(days=random.randrange(days))
    qty = random.choices([1, 2, 3], [7, 2, 1])[0]
    status = random.choices(["完了", "キャンセル"], [92, 8])[0]
    orders.append((f"O{i:06d}", order_date, cust[0], prod[0], prod[1], prod[2], qty, prod[3], qty * prod[3], status))

spark.createDataFrame(
    orders,
    "order_id string, order_date date, customer_id string, product_id string, product_name string, "
    "category string, quantity int, unit_price int, amount int, status string",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("orders")

spark.createDataFrame(
    customers, "customer_id string, prefecture string, member_rank string"
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("customers")

display(spark.table("orders").limit(10))
show_link("orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2. 集計した表の落とし穴を確かめる
# MAGIC
# MAGIC 実演でお見せした場面を、自分の環境で再現します。
# MAGIC
# MAGIC ダッシュボードから毎回6,000行を読むのは重いので、毎晩バッチで日次サマリを作っているとします。日ごとに、売上合計、注文数、その日に買った人の数を入れた表です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 毎晩作っている想定の日次サマリ。単位は「日」。
# MAGIC DROP TABLE IF EXISTS daily_sales_summary;
# MAGIC CREATE TABLE daily_sales_summary AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   SUM(amount)                 AS total_sales,
# MAGIC   COUNT(*)                    AS order_count,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM orders
# MAGIC WHERE status = '完了'
# MAGIC GROUP BY order_date;
# MAGIC
# MAGIC SELECT * FROM daily_sales_summary
# MAGIC WHERE order_date BETWEEN '2026-03-01' AND '2026-03-05'
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ここで「3月は何人のお客さんが買ってくれましたか」と聞かれました。手元にあるのは31日ぶんの行です。
# MAGIC
# MAGIC 聞かれているのは、3月に1回でも買った人の数です。同じ人は何回買っても1人と数えます。日次サマリの `unique_customers` も同じ定義で、その日に買った人の数です。
# MAGIC
# MAGIC ✍️ 予想: 日ごとに数えた「その日に買った人の数」を31日ぶん足したら、「3月に1回でも買った人の数」になるでしょうか。売上合計と注文数はどうでしょうか。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 上が日次サマリから足した値、下が注文データから直接数えた値
# MAGIC SELECT
# MAGIC   'サマリから足した値'        AS how,
# MAGIC   SUM(total_sales)            AS total_sales,
# MAGIC   SUM(order_count)            AS order_count,
# MAGIC   SUM(unique_customers)       AS unique_customers
# MAGIC FROM daily_sales_summary
# MAGIC WHERE order_date BETWEEN '2026-03-01' AND '2026-03-31'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   '注文データの値'            AS how,
# MAGIC   SUM(amount)                 AS total_sales,
# MAGIC   COUNT(*)                    AS order_count,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM orders
# MAGIC WHERE status = '完了'
# MAGIC   AND order_date BETWEEN '2026-03-01' AND '2026-03-31';

# COMMAND ----------

# MAGIC %md
# MAGIC 売上合計は1,721,180円、注文数は339件で、どちらも一致します。購入顧客数だけが合いません。サマリから足すと335人、注文データから数えると246人です。
# MAGIC
# MAGIC 3月に2日以上買った人が、買った日数ぶん数えられているためです。内訳を見てみます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- まず、お客さんごとに「3月に何日買ったか」を数える
# MAGIC WITH `顧客ごとの購入日数` AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(DISTINCT order_date) AS `購入日数`
# MAGIC   FROM orders
# MAGIC   WHERE status = '完了'
# MAGIC     AND order_date BETWEEN '2026-03-01' AND '2026-03-31'
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC -- 次に、購入日数ごとに人数を数える
# MAGIC SELECT
# MAGIC   `購入日数`,
# MAGIC   COUNT(*) AS `人数`
# MAGIC FROM `顧客ごとの購入日数`
# MAGIC GROUP BY `購入日数`
# MAGIC ORDER BY `購入日数`;

# COMMAND ----------

# MAGIC %md
# MAGIC 1日だけ買った人が173人、2日が59人、3日が12人、4日が2人でした。2日以上買った人は73人です。
# MAGIC
# MAGIC 重複ぶんは 59 + 12 × 2 + 2 × 3 = 89人。335 − 246 = 89 と一致します。
# MAGIC
# MAGIC ### なぜ購入顧客数だけが合わないのか
# MAGIC
# MAGIC 日次サマリの行に入っているのは、その日に買った人の数だけです。3月1日は7人、3月2日は10人、というように数だけが残っています。この7人と10人に同じ人がいるかどうかは、この表からは分かりません。誰が買ったのかは、集計した時点で消えているからです。
# MAGIC
# MAGIC 売上合計と注文数が合うのは、重なりようがないからです。3月1日の売上と3月2日の売上は別のお金で、同じ注文が2つの日に入ることもありません。
# MAGIC
# MAGIC ### 集計した表からまとめ直すとき、指標は3種類に分かれます
# MAGIC
# MAGIC | 種類 | 例 | 日次サマリから月の値を出せるか |
# MAGIC |---|---|---|
# MAGIC | そのまま足せる | 売上合計、注文数 | 出せる。足すだけ |
# MAGIC | 分子と分母が残っていれば出せる | 客単価、キャンセル率 | 売上合計 ÷ 注文数 で正しく出る。逆に、集計した表に客単価の列だけを残すと出せなくなる |
# MAGIC | そもそも計算できない | 購入顧客数、中央値 | 出せない。誰が買ったのかが残っていない |
# MAGIC
# MAGIC いま確かめたのは3つ目です。これは日次サマリに限りません。ダッシュボードの合計行、フィルタを外したとき、Excelに落として足したときも同じです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3. メトリクスビューを作る
# MAGIC
# MAGIC ここからが本題です。集計した表を作るのをやめて、計算方法だけを登録します。
# MAGIC
# MAGIC ### 登録するものは、フィールドとメジャーの2種類です
# MAGIC
# MAGIC メトリクスビューの定義は、この2つを並べたものです。先にこの2つの違いを押さえてください。ここが分かれば、あとは書き写すだけになります。
# MAGIC
# MAGIC **フィールド**は、集計する単位に使える列です。注文日、商品カテゴリ、都道府県、会員ランクなどです。もとのテーブルの1行1行に値としてそのまま入っているので、集計しなくても取り出せます。「カテゴリごとに」「都道府県ごとに」と切り分けるときに使う列、と考えてください。YAMLでは `fields` に書きます。
# MAGIC
# MAGIC **メジャー**は、集計のしかたを書いたものです。売上合計なら `SUM(amount)`、注文数なら `COUNT(*)`、購入顧客数なら `COUNT(DISTINCT customer_id)` です。式だけを登録しておき、値は計算しません。YAMLでは `measures` に書きます。
# MAGIC
# MAGIC この違いが、そのまま Part 2 で見た問題の答えになっています。
# MAGIC
# MAGIC | | フィールド | メジャー |
# MAGIC |---|---|---|
# MAGIC | 中身 | もとの行に入っている値 | 集計のしかた (式) |
# MAGIC | 例 | 注文日、カテゴリ、都道府県 | 売上合計、注文数、購入顧客数 |
# MAGIC | 値が決まるとき | 行を見た時点で決まっている | 集計する単位が決まってはじめて決まる |
# MAGIC | YAMLのキー | `fields` | `measures` |
# MAGIC | 画面の見出し | フィールド | メジャー |
# MAGIC
# MAGIC 3月の購入顧客数が246人なのか335人なのかは、「3月全体で数えるのか、日ごとに数えてから足すのか」が決まらないと決まりませんでした。メジャーが単位なしでは値を持てないのは、これと同じことです。だからメトリクスビューは値を持たず、聞かれてから計算します。
# MAGIC
# MAGIC 他のBIツールを使ってきた方へ。ここで言うフィールドは、一般に「ディメンション」と呼ばれてきたものと同じです。英語ドキュメントにも "Fields, also called dimensions" という言い添えがあります。ただしDatabricksの画面もドキュメントも「フィールド」で通っているので、このノートブックも「フィールド」で統一します。YAMLのキーとしては `dimensions` も受け付けられますが、これは後方互換性のための同義語です。新しく書くときは `fields` を使ってください。
# MAGIC
# MAGIC 参考: [メトリクスビューの基本的なモデリング](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/basic-modeling)
# MAGIC
# MAGIC ### YAMLを読む
# MAGIC
# MAGIC 次のセルは完成しています。実行する前に、YAMLの中身を上から読んでみてください。
# MAGIC
# MAGIC | 要素 | 何を書くか |
# MAGIC |---|---|
# MAGIC | `source` | 元になるテーブル。ここでは `orders` |
# MAGIC | `filter` | 誰がクエリしても必ずかかる条件。ここでは完了した注文だけ |
# MAGIC | `joins` | 定義の中で結合しておく。使う人は結合を書かずに都道府県や会員ランクを使える |
# MAGIC | `fields` | フィールド。注文日、注文月、カテゴリ、都道府県、会員ランク |
# MAGIC | `measures` | メジャー。売上合計、注文数、客単価、購入顧客数 |
# MAGIC
# MAGIC YAMLの読み方
# MAGIC
# MAGIC - 行頭のスペース2つで、階層を表します
# MAGIC - `- name:` の先頭の `-` は、リストの1項目という意味です。フィールドやメジャーを並べるのに使います
# MAGIC - `name` は呼び名、`expr` は中身の式です。この2つが対になります
# MAGIC - `$$` で囲んだ中がYAMLです。`CREATE VIEW ... WITH METRICS LANGUAGE YAML AS $$ ... $$` という形で登録します
# MAGIC - `'on'` にだけクォートが付いているのは、YAMLでは `on` が「真偽値の true」と解釈されることがあるためです
# MAGIC
# MAGIC ✍️ 予想: この定義の中に `GROUP BY` は出てきません。では、集計する単位はいつ決まるのでしょうか。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sales_metrics
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC version: 1.1
# MAGIC comment: "もくもく商店の売上指標"
# MAGIC source: orders
# MAGIC filter: status = '完了'
# MAGIC
# MAGIC joins:
# MAGIC   - name: customer
# MAGIC     source: customers
# MAGIC     'on': source.customer_id = customer.customer_id
# MAGIC
# MAGIC fields:
# MAGIC   - name: order_date
# MAGIC     expr: order_date
# MAGIC     comment: "注文日"
# MAGIC   - name: order_month
# MAGIC     expr: DATE_TRUNC('MONTH', order_date)
# MAGIC     comment: "注文月"
# MAGIC   - name: category
# MAGIC     expr: category
# MAGIC     comment: "商品カテゴリ"
# MAGIC   - name: prefecture
# MAGIC     expr: customer.prefecture
# MAGIC     comment: "顧客の都道府県"
# MAGIC   - name: member_rank
# MAGIC     expr: customer.member_rank
# MAGIC     comment: "会員ランク"
# MAGIC
# MAGIC measures:
# MAGIC   - name: total_sales
# MAGIC     expr: SUM(amount)
# MAGIC     comment: "売上合計 (円)"
# MAGIC   - name: order_count
# MAGIC     expr: COUNT(*)
# MAGIC     comment: "注文数"
# MAGIC   - name: avg_order_value
# MAGIC     expr: MEASURE(total_sales) / MEASURE(order_count)
# MAGIC     comment: "客単価 (1注文あたりの金額)"
# MAGIC   - name: unique_customers
# MAGIC     expr: COUNT(DISTINCT customer_id)
# MAGIC     comment: "購入顧客数"
# MAGIC $$;

# COMMAND ----------

show_link("sales_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC リンク先のカタログエクスプローラーで、`sales_metrics` の中身を見てみてください。「メジャー (4)」と「フィールド (5)」という2つの見出しで、いま登録したものが一覧になっています。右側には `source` に書いた `orders` と、`filter` に書いた `status = '完了'` も表示されます。
# MAGIC
# MAGIC 見出しの並び方が、YAMLの構造とそのまま対応しています。YAMLで `measures` に書いたものが「メジャー」、`fields` に書いたものが「フィールド」です。
# MAGIC
# MAGIC 客単価の定義は `MEASURE(total_sales) / MEASURE(order_count)` です。売上合計と注文数を、それぞれメジャーとして持っておき、割り算はあとから行います。Part 2 で見たとおり、割り算の結果だけを残すと、別の単位では出せなくなるためです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4. 軸を変えて聞く
# MAGIC
# MAGIC ここからが今日いちばん手を動かすところです。ビューは1本のまま、`SELECT` と `GROUP BY` を書き換えて、いろいろな単位で聞きます。
# MAGIC
# MAGIC 書き方のきまりは2つだけです。どちらも Part 3 のフィールドとメジャーの話から出てきます。
# MAGIC
# MAGIC - 集計する単位は `GROUP BY` に書きます。ここに書くのはフィールドです
# MAGIC - メジャーは `MEASURE()` で囲みます。単位が決まったので、ここで値が計算されます
# MAGIC
# MAGIC まず、見本を1つ実行します。月ごとの売上合計と客単価です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 見本: 月ごと
# MAGIC SELECT
# MAGIC   order_month,
# MAGIC   MEASURE(total_sales)            AS total_sales,
# MAGIC   ROUND(MEASURE(avg_order_value)) AS avg_order_value
# MAGIC FROM sales_metrics
# MAGIC WHERE order_month >= '2026-01-01'
# MAGIC GROUP BY order_month
# MAGIC ORDER BY order_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 課題1: カテゴリごとに聞く
# MAGIC
# MAGIC 見本を書き換えて、商品カテゴリごとの売上合計と客単価を出してください。期間の絞り込みは外して、全期間で構いません。
# MAGIC
# MAGIC ヒント: 変えるのは3か所です。`SELECT` の1列目、`GROUP BY`、`ORDER BY`。フィールド名は `category` です。
# MAGIC
# MAGIC ✍️ 予想: どのカテゴリの客単価がいちばん高くなるでしょうか。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 課題1: ここに書いてください

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><b>課題1の解答 (自分で書いてから開いてください)</b></summary>
# MAGIC <pre>
# MAGIC SELECT
# MAGIC   category,
# MAGIC   MEASURE(total_sales)            AS total_sales,
# MAGIC   ROUND(MEASURE(avg_order_value)) AS avg_order_value
# MAGIC FROM sales_metrics
# MAGIC GROUP BY category
# MAGIC ORDER BY total_sales DESC;
# MAGIC </pre>
# MAGIC <p>客単価がいちばん高いのは家電です。1件あたりの金額が大きく、件数は少ないカテゴリです。</p>
# MAGIC <p>ビューは作り直していません。GROUP BY を order_month から category に変えただけです。標準ビューなら、ここで2本目のビューを作ることになっていました。</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 課題2: 都道府県ごとに聞く
# MAGIC
# MAGIC 次は、都道府県ごとの売上合計と購入顧客数を出してください。
# MAGIC
# MAGIC 都道府県は `customers` テーブルの列ですが、`JOIN` は書きません。メトリクスビューの定義の中で結合してあるので、`prefecture` をそのまま使えます。
# MAGIC
# MAGIC ヒント: 購入顧客数のメジャー名は `unique_customers` です。
# MAGIC
# MAGIC ✍️ 予想: `JOIN` を書かずに都道府県で集計できるでしょうか。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 課題2: ここに書いてください

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><b>課題2の解答 (自分で書いてから開いてください)</b></summary>
# MAGIC <pre>
# MAGIC SELECT
# MAGIC   prefecture,
# MAGIC   MEASURE(total_sales)      AS total_sales,
# MAGIC   MEASURE(unique_customers) AS unique_customers
# MAGIC FROM sales_metrics
# MAGIC GROUP BY prefecture
# MAGIC ORDER BY total_sales DESC;
# MAGIC </pre>
# MAGIC <p>JOIN は書きません。メトリクスビューの joins に結合を書いてあるので、使う人は prefecture をほかのフィールドと同じように扱えます。</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 課題3: 3月の購入顧客数を聞く
# MAGIC
# MAGIC Part 2 と同じ質問に戻ります。2026年3月の購入顧客数を、メトリクスビューから出してください。
# MAGIC
# MAGIC ヒント: 3月全体の1行だけが欲しいので、`GROUP BY` は要りません。期間は `WHERE order_date BETWEEN '2026-03-01' AND '2026-03-31'` で絞ります。
# MAGIC
# MAGIC ✍️ 予想: 日次サマリから足した335人と、注文データから数えた246人の、どちらが返るでしょうか。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 課題3: ここに書いてください

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><b>課題3の解答 (自分で書いてから開いてください)</b></summary>
# MAGIC <pre>
# MAGIC SELECT
# MAGIC   MEASURE(total_sales)      AS total_sales,
# MAGIC   MEASURE(order_count)      AS order_count,
# MAGIC   MEASURE(unique_customers) AS unique_customers
# MAGIC FROM sales_metrics
# MAGIC WHERE order_date BETWEEN '2026-03-01' AND '2026-03-31';
# MAGIC </pre>
# MAGIC <p>246人が返ります。注文データから数えた値と同じです。</p>
# MAGIC <p>メトリクスビューは集計した表を持っていません。聞かれた範囲を orders から数え直すので、Part 2 のような重複が起きません。GROUP BY を書かなければ、WHERE で絞った範囲の全体が1行で返ります。</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 確認: SELECT * はエラーになる
# MAGIC
# MAGIC ここは書くところではありません。次のセルをそのまま実行してください。**わざとエラーを出します。**
# MAGIC
# MAGIC ハンズオン中に自分で書いていて必ず出会うエラーなので、先に見ておきます。
# MAGIC
# MAGIC ✍️ 予想: なぜエラーになるのでしょうか。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- そのまま実行してください (エラーになります)
# MAGIC SELECT * FROM sales_metrics;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><b>エラーの意味 (予想を書いてから開いてください)</b></summary>
# MAGIC <p>出るのはこのエラーです。</p>
# MAGIC <pre>
# MAGIC [METRIC_VIEW_MISSING_MEASURE_FUNCTION] The usage of measure column
# MAGIC [total_sales,order_count,avg_order_value,unique_customers] of a metric view
# MAGIC requires a MEASURE() (or AGG()) function to produce results.
# MAGIC </pre>
# MAGIC <p>エラーの文に並んでいる4つは、すべてメジャーです。メジャーは集計のしかたを書いたものなので、どの単位で集計するかが決まらないと値が決まりません。SELECT * は単位を指定していないので、値を作れません。</p>
# MAGIC <p>欲しい列を明示して、メジャーは MEASURE() で囲む。これがメトリクスビューへの聞き方です。</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### ここまでできていればOKです
# MAGIC
# MAGIC 1本のメトリクスビューに、月、カテゴリ、都道府県、期間全体と、4通りの聞き方をしました。ビューは一度も作り直していません。
# MAGIC
# MAGIC Part 2 で見た「集計した表を配ると、別の単位では正しく出せない」という問題が、ここでは起きていません。集計した表を持たず、聞かれた単位でそのつど `orders` から計算しているためです。
# MAGIC
# MAGIC 時間が余っている方は、この先に進んでください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5. ほかのツールから使ってみる (時間があれば)
# MAGIC
# MAGIC メトリクスビューは、SQLを書かない人にも使ってもらえます。画面から次の2つを試せます。
# MAGIC
# MAGIC ### AI/BI ダッシュボード
# MAGIC
# MAGIC 1. 新しいダッシュボードを作り、データとして `sales_metrics` を追加します
# MAGIC 2. 棒グラフを追加し、X軸に `category`、Y軸に `total_sales` を選びます
# MAGIC 3. `MEASURE()` を書かなくても値が出ることを確かめます
# MAGIC
# MAGIC ### Genie
# MAGIC
# MAGIC 1. 新しい Genie スペースを作り、データとして `sales_metrics` を追加します
# MAGIC 2. 「2026年3月は何人のお客さんが買いましたか」と日本語で聞きます
# MAGIC 3. 課題3と同じ246人が返ること、生成されたSQLに `MEASURE()` が使われていることを確かめます
# MAGIC
# MAGIC 参考: [メトリクスビューのクエリー (公式ドキュメント)](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## おまけ Part 6. メジャーを1つ足してみる
# MAGIC
# MAGIC ここからは持ち帰り用です。自分で定義を書く練習になります。
# MAGIC
# MAGIC ### 課題4: 購入者あたり注文数を足す
# MAGIC
# MAGIC 「購入者あたり注文数」を、メジャーとして足してください。1人のお客さんが平均で何回買ったか、という指標です。式は「注文数 ÷ 購入顧客数」です。
# MAGIC
# MAGIC 次のセルには Part 3 と同じ定義が入っています。`measures` の最後に3行足して、実行してください。
# MAGIC
# MAGIC ```yaml
# MAGIC   - name: orders_per_customer
# MAGIC     expr: (ここに式を書く)
# MAGIC     comment: "購入者あたり注文数"
# MAGIC ```
# MAGIC
# MAGIC ヒント: 客単価の行と同じ形です。注文数のメジャー名は `order_count`、購入顧客数のメジャー名は `unique_customers` です。どちらも `MEASURE()` で囲んで参照します。
# MAGIC
# MAGIC ✍️ 予想: 3月の注文数は339件、購入顧客数は246人でした。購入者あたり注文数はいくつになるでしょうか。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sales_metrics
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC version: 1.1
# MAGIC comment: "もくもく商店の売上指標"
# MAGIC source: orders
# MAGIC filter: status = '完了'
# MAGIC
# MAGIC joins:
# MAGIC   - name: customer
# MAGIC     source: customers
# MAGIC     'on': source.customer_id = customer.customer_id
# MAGIC
# MAGIC fields:
# MAGIC   - name: order_date
# MAGIC     expr: order_date
# MAGIC     comment: "注文日"
# MAGIC   - name: order_month
# MAGIC     expr: DATE_TRUNC('MONTH', order_date)
# MAGIC     comment: "注文月"
# MAGIC   - name: category
# MAGIC     expr: category
# MAGIC     comment: "商品カテゴリ"
# MAGIC   - name: prefecture
# MAGIC     expr: customer.prefecture
# MAGIC     comment: "顧客の都道府県"
# MAGIC   - name: member_rank
# MAGIC     expr: customer.member_rank
# MAGIC     comment: "会員ランク"
# MAGIC
# MAGIC measures:
# MAGIC   - name: total_sales
# MAGIC     expr: SUM(amount)
# MAGIC     comment: "売上合計 (円)"
# MAGIC   - name: order_count
# MAGIC     expr: COUNT(*)
# MAGIC     comment: "注文数"
# MAGIC   - name: avg_order_value
# MAGIC     expr: MEASURE(total_sales) / MEASURE(order_count)
# MAGIC     comment: "客単価 (1注文あたりの金額)"
# MAGIC   - name: unique_customers
# MAGIC     expr: COUNT(DISTINCT customer_id)
# MAGIC     comment: "購入顧客数"
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC 足せたら、会員ランクごとに聞いてみてください。ゴールド会員がよく買っているかどうかが分かります。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 課題4の確認: ここに書いてください

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><b>課題4の解答 (自分で書いてから開いてください)</b></summary>
# MAGIC <p>measures の最後に足す3行です。</p>
# MAGIC <pre>
# MAGIC   - name: orders_per_customer
# MAGIC     expr: MEASURE(order_count) / MEASURE(unique_customers)
# MAGIC     comment: "購入者あたり注文数"
# MAGIC </pre>
# MAGIC <p>3月なら 339件 ÷ 246人 で、1.38回です。会員ランクごとに聞くとこうなります。</p>
# MAGIC <pre>
# MAGIC SELECT
# MAGIC   member_rank,
# MAGIC   MEASURE(order_count)                   AS order_count,
# MAGIC   MEASURE(unique_customers)              AS unique_customers,
# MAGIC   ROUND(MEASURE(orders_per_customer), 2) AS orders_per_customer
# MAGIC FROM sales_metrics
# MAGIC GROUP BY member_rank
# MAGIC ORDER BY order_count DESC;
# MAGIC </pre>
# MAGIC <p>客単価と同じで、分子と分母をそれぞれメジャーとして持ち、割り算はあとから行います。この書き方なら、会員ランクごとに聞けば会員ランクごとの注文数と購入顧客数から計算されます。割り算の結果を先に持っていると、こうはなりません。</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 読み物: ほかにもできること
# MAGIC
# MAGIC 今日は扱いませんが、メトリクスビューにはこういう機能もあります。
# MAGIC
# MAGIC **表示名・同義語・書式**
# MAGIC
# MAGIC `display_name` (画面に出す名前)、`synonyms` (別名)、`format` (通貨や小数点の書式) を付けておくと、ダッシュボードの表示や Genie の質問理解に使われます。英語の列名に日本語の表示名と同義語を付けておくと、「売上」「売り上げ」「売上高」のような言い方の揺れにも対応しやすくなります。
# MAGIC
# MAGIC ```yaml
# MAGIC   - name: total_sales
# MAGIC     expr: SUM(amount)
# MAGIC     display_name: "売上合計"
# MAGIC     synonyms: ["売上", "売り上げ", "売上高"]
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: JPY
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC ```
# MAGIC
# MAGIC **ウィンドウメジャー**
# MAGIC
# MAGIC 「過去7日間の購入顧客数」や「前年同月の売上」のような、期間をずらしたり広げたりする指標も定義できます。
# MAGIC
# MAGIC ```yaml
# MAGIC   - name: t7d_customers
# MAGIC     expr: COUNT(DISTINCT customer_id)
# MAGIC     window:
# MAGIC       - order: order_date
# MAGIC         range: trailing 7 day
# MAGIC         semiadditive: last
# MAGIC ```
# MAGIC
# MAGIC `trailing 7 day` は、既定では当日を含まない前日までの7日間です。当日を含めたいときは `trailing 7 day inclusive` と書きます。
# MAGIC
# MAGIC **パラメーターとマテリアライズ**
# MAGIC
# MAGIC - パラメーター: クエリ時に値を渡して計算を変える。[パラメーターの使用](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/use-parameters)
# MAGIC - マテリアライズ: よく使う集計を事前計算しておき、クエリを自動で速いほうに振り分ける。[マテリアライズ](https://docs.databricks.com/aws/ja/uc-semantics/metric-views/materialization)
# MAGIC
# MAGIC **クエリ時の結合**
# MAGIC
# MAGIC メトリクスビューは、クエリのときにほかのテーブルと直接 `JOIN` できません。組み合わせたいときは、いったん `WITH` 句で結果を受けてから結合します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ: 今日作ったものはどこにつながるか
# MAGIC
# MAGIC メトリクスビューは、SQLを書く人のための仕組みであると同時に、AIに正しい数字を答えさせるための土台でもあります。
# MAGIC
# MAGIC Databricks は、社内で使う言葉や指標の意味をまとめる仕組みを「Genieオントロジー」と呼んでいます (プレビュー)。中身は2階建てです。
# MAGIC
# MAGIC | 誰が用意するか | 中身 |
# MAGIC |---|---|
# MAGIC | 人が定義する | メトリクスビュー (指標)、ドメイン (データのまとまり)、ページ (業務の言葉の説明) |
# MAGIC | Databricks が集める | 既存のノートブックやクエリから拾った定義や、よく使われている情報 |
# MAGIC
# MAGIC 今日作ったメトリクスビューは、人が定義する部分にあたります。購入顧客数の定義を1か所に置いたので、SQLで聞いても、ダッシュボードで見ても、Genie に日本語で聞いても同じ246人が返ります。
# MAGIC
# MAGIC - [Genie オントロジー](https://docs.databricks.com/aws/ja/genie/genie-ontology)
# MAGIC - [Unity Catalog セマンティクス](https://docs.databricks.com/aws/ja/uc-semantics/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 後片付け (任意)
# MAGIC
# MAGIC 作ったものを消したいときは、次のセルのコメントを外して実行してください。

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
