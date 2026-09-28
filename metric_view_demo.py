# Databricks notebook source
# MAGIC %md
# MAGIC # メトリクスビューを見てみる
# MAGIC
# MAGIC 架空のECショップ「もくもく商店」のデータで、集計した表を配るやり方と、メトリクスビューを比べます。
# MAGIC
# MAGIC **メトリクスビューを一言で言うと、集計した表ではなく、指標の計算方法を登録しておくビューです。** 数字そのものは持たず、どの単位で計算するかは聞く人が決めます。
# MAGIC
# MAGIC Part 0 と Part 1 は、データを用意するところです。当日までに実行済みにしておきます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0. 準備

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "カタログ")
dbutils.widgets.text("schema", "jedai_metric_view_demo", "スキーマ")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"作業場所: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1. 題材データを作る
# MAGIC
# MAGIC 参加者のノートブックと同じコード、同じ乱数シードです。出てくる数字も同じになります。

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

print("orders と customers を作成しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 注文データ
# MAGIC
# MAGIC 1行が1注文です。2025年1月から2026年6月まで、6,000行あります。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS `注文件数`, MIN(order_date) AS `最初の注文`, MAX(order_date) AS `最後の注文` FROM orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 最初のビューを作る
# MAGIC
# MAGIC 「月ごとの売上と客単価が見たい」と言われました。毎回SQLを書かせるのは大変なので、ビューにして配ります。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1本目。月ごとの売上と客単価。
# MAGIC -- GROUP BY に書いた「月」が、このビューの単位になる。
# MAGIC CREATE OR REPLACE VIEW v_sales_monthly AS
# MAGIC SELECT
# MAGIC   DATE_TRUNC('MONTH', order_date)  AS order_month,
# MAGIC   SUM(amount)                      AS total_sales,
# MAGIC   COUNT(*)                         AS order_count,
# MAGIC   SUM(amount) / COUNT(*)           AS avg_order_value,
# MAGIC   COUNT(DISTINCT customer_id)      AS unique_customers
# MAGIC FROM orders
# MAGIC WHERE status = '完了'
# MAGIC GROUP BY DATE_TRUNC('MONTH', order_date);
# MAGIC
# MAGIC SELECT * FROM v_sales_monthly ORDER BY order_month DESC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 軸が増えるたびにビューが増える
# MAGIC
# MAGIC 「カテゴリ別でも見たい」「都道府県別も」と言われました。そのたびにビューを作ることになります。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2本目。単位は「月 × カテゴリ」。
# MAGIC -- 客単価の計算式 SUM(amount) / COUNT(*) が、ここにも書かれる。
# MAGIC CREATE OR REPLACE VIEW v_sales_monthly_category AS
# MAGIC SELECT
# MAGIC   DATE_TRUNC('MONTH', order_date)  AS order_month,
# MAGIC   category,
# MAGIC   SUM(amount)                      AS total_sales,
# MAGIC   COUNT(*)                         AS order_count,
# MAGIC   SUM(amount) / COUNT(*)           AS avg_order_value,
# MAGIC   COUNT(DISTINCT customer_id)      AS unique_customers
# MAGIC FROM orders
# MAGIC WHERE status = '完了'
# MAGIC GROUP BY DATE_TRUNC('MONTH', order_date), category;
# MAGIC
# MAGIC SELECT * FROM v_sales_monthly_category WHERE order_month = '2026-03-01' ORDER BY category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3本目。単位は「月 × 都道府県」。顧客テーブルとの結合も要る。
# MAGIC -- 客単価の計算式が3か所目。
# MAGIC CREATE OR REPLACE VIEW v_sales_monthly_prefecture AS
# MAGIC SELECT
# MAGIC   DATE_TRUNC('MONTH', o.order_date) AS order_month,
# MAGIC   c.prefecture,
# MAGIC   SUM(o.amount)                     AS total_sales,
# MAGIC   COUNT(*)                          AS order_count,
# MAGIC   SUM(o.amount) / COUNT(*)          AS avg_order_value,
# MAGIC   COUNT(DISTINCT o.customer_id)     AS unique_customers
# MAGIC FROM orders o
# MAGIC JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.status = '完了'
# MAGIC GROUP BY DATE_TRUNC('MONTH', o.order_date), c.prefecture;
# MAGIC
# MAGIC SELECT * FROM v_sales_monthly_prefecture WHERE order_month = '2026-03-01' ORDER BY total_sales DESC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 軸を1つ増やすたびに、ビューが1本増える。
# MAGIC SHOW VIEWS;

# COMMAND ----------

# MAGIC %md
# MAGIC ビューが3本になりました。
# MAGIC
# MAGIC - 客単価の計算式が3本に書かれています。返品を差し引く形に変えたくなったら、3本とも直します
# MAGIC - 軸の組み合わせはまだあります。月 × カテゴリ × 都道府県、会員ランク別、商品別。そのたびに1本増えます
# MAGIC - このビューを使う人が3人いれば、3人ともそれぞれSQLを書きます。その時点で客単価の計算式は3人ぶんに分かれます
# MAGIC - そして、どのビューも使うにはSQLが要ります

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 集計した表から出し直すと、値まで狂う
# MAGIC
# MAGIC ビューが増えるのは手間の問題です。もうひとつ、値そのものが狂う問題があります。
# MAGIC
# MAGIC ダッシュボードから毎回6,000行を読むのは重いので、毎晩バッチで日次サマリを作っているとします。よくある構成です。

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
# MAGIC ここで「3月は何人のお客さんが買ってくれましたか」と聞かれます。手元にあるのは31日ぶんの行です。
# MAGIC
# MAGIC 聞かれているのは、3月に1回でも買った人の数です。同じ人は何回買っても1人と数えます。日次サマリの `unique_customers` も同じ定義で、その日に買った人の数です。
# MAGIC
# MAGIC 日ごとに数えた「その日に買った人の数」を31日ぶん足したら、「3月に1回でも買った人の数」になるでしょうか。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 日次サマリから3月をまとめる
# MAGIC SELECT
# MAGIC   SUM(total_sales)      AS `売上合計`,
# MAGIC   SUM(order_count)      AS `注文数`,
# MAGIC   SUM(unique_customers) AS `購入顧客数`
# MAGIC FROM daily_sales_summary
# MAGIC WHERE order_date BETWEEN '2026-03-01' AND '2026-03-31';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 注文データから直接数える (こちらが正しい)
# MAGIC SELECT
# MAGIC   SUM(amount)                 AS `売上合計`,
# MAGIC   COUNT(*)                    AS `注文数`,
# MAGIC   COUNT(DISTINCT customer_id) AS `購入顧客数`
# MAGIC FROM orders
# MAGIC WHERE status = '完了'
# MAGIC   AND order_date BETWEEN '2026-03-01' AND '2026-03-31';

# COMMAND ----------

# MAGIC %md
# MAGIC 売上合計と注文数は一致します。購入顧客数だけが合いません。
# MAGIC
# MAGIC - 日次サマリから足すと **335人**
# MAGIC - 実際に買ったのは **246人**
# MAGIC
# MAGIC 3月に2日以上買った人が、買った日数ぶん数えられているためです。内訳を見てみます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3月に何日買ったか、人数の内訳
# MAGIC
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
# MAGIC ### なぜ購入顧客数が合わないのか
# MAGIC
# MAGIC - 日次サマリの行に入っているのは、その日に買った人の数だけです。3月1日は7人、3月2日は10人、というように数だけが残っています
# MAGIC - この7人と10人に同じ人がいるかどうかは、この表からは分かりません。誰が買ったのかは、集計した時点で消えているからです
# MAGIC - 実際には、3月に2日以上買ったお客さんが73人いました。4日買った人も2人います。その人たちは、日ごとの「その日に買った人の数」に4回入っています
# MAGIC - だから31日ぶんを足した335人には、同じ人が何度も入っています。正しい246人との差の89人が、その重複ぶんです
# MAGIC
# MAGIC ### なぜ売上合計と注文数は合うのか
# MAGIC
# MAGIC - 3月1日の売上と3月2日の売上は、別のお金です。同じ注文が2つの日に入ることもありません
# MAGIC - 重なりようがないので、足せば正しい値になります
# MAGIC
# MAGIC 集計した表からまとめ直すとき、指標は3種類に分かれます。
# MAGIC
# MAGIC | 種類 | 例 | 日次サマリから月の値を出せるか |
# MAGIC |---|---|---|
# MAGIC | そのまま足せる | 売上合計、注文数 | 出せる。足すだけ |
# MAGIC | 分子と分母が残っていれば出せる | 客単価、キャンセル率 | 売上合計 1,721,180円 ÷ 注文数 339件 で 5,077円。逆に、集計した表に客単価の列だけを残すと出せなくなる |
# MAGIC | そもそも計算できない | 購入顧客数、中央値 | 出せない。誰が買ったのかが残っていない |
# MAGIC
# MAGIC - いま見たのは3つ目です
# MAGIC - これは日次サマリに限りません。ダッシュボードの合計行、フィルタを外したとき、Excelに落として足したとき、どれでも同じことが起きます
# MAGIC
# MAGIC ### 客単価の列だけを残すと、月の客単価は出せない
# MAGIC
# MAGIC 客単価は「その日の売上合計 ÷ その日の注文数」です。日をまたいで合わせるには、それぞれの日が何件の注文から出た値かが要ります。
# MAGIC
# MAGIC 3月1日の客単価が2,206円、3月2日が10,886円だったとします。この2つの数字だけでは、2日間の客単価は決まりません。
# MAGIC
# MAGIC | もし注文数が | 2日間の売上 | 2日間の注文数 | 2日間の客単価 |
# MAGIC |---|---|---|---|
# MAGIC | 7件と10件 | 15,440 + 108,860 = 124,300円 | 17件 | 7,312円 |
# MAGIC | 100件と1件 | 220,600 + 10,886 = 231,486円 | 101件 | 2,292円 |
# MAGIC
# MAGIC 同じ2,206円と10,886円でも、件数が違えば答えが変わります。客単価の列しか残っていないと、どちらなのか判断できません。
# MAGIC
# MAGIC 集計した表に残すべきなのは、割り算の結果ではなく、分子の売上合計と分母の注文数です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 困りごとは、どちらも同じところから来ている
# MAGIC
# MAGIC - ここまでの困りごとは、どちらも **集計した表を共有したこと** から来ています
# MAGIC - 数字は、集計した時点で単位が決まります。別の単位で見たくなったら、ビューを作り直すか、足し直して間違えるかのどちらかです
# MAGIC - では表を共有しなければいいかというと、見たい人に毎回SQLを書かせるわけにもいきません
# MAGIC - 共有するものを、集計した表ではなく **計算方法** に変えられないか
# MAGIC
# MAGIC それがメトリクスビューです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. メトリクスビューを作る
# MAGIC
# MAGIC 集計した表は作らず、計算方法だけを登録します。登録するものは2種類です。
# MAGIC
# MAGIC - **フィールド** (`fields`): 集計する単位に使える列。注文日、カテゴリ、都道府県、会員ランク。もとの行に値として入っている
# MAGIC - **メジャー** (`measures`): 集計のしかたを書いたもの。売上合計、注文数、購入顧客数。集計する単位が決まってはじめて値が決まる
# MAGIC
# MAGIC GROUP BY は一度も出てきません。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 集計した表は作らず、計算方法だけを登録する。
# MAGIC --   source   : 元になるテーブル
# MAGIC --   filter   : 誰が使っても必ずかかる条件
# MAGIC --   joins    : 定義の中で結合しておく。使う側は結合を意識しない
# MAGIC --   fields   : フィールド。集計する単位に使える列
# MAGIC --   measures : メジャー。集計のしかた
# MAGIC -- GROUP BY が一度も出てこない。どの単位で集計するかは、クエリする人が決める。
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
# MAGIC     comment: "客単価"
# MAGIC   - name: unique_customers
# MAGIC     expr: COUNT(DISTINCT customer_id)
# MAGIC     comment: "購入顧客数"
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 軸を変えて聞く
# MAGIC
# MAGIC さっきは軸ごとに3本のビューを作りました。ここでは1本のまま、GROUP BY だけを変えます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 月ごと (1本目のビューに相当)。GROUP BY に書くのがフィールド、MEASURE() で囲むのがメジャー
# MAGIC SELECT order_month, MEASURE(total_sales) AS `売上合計`, ROUND(MEASURE(avg_order_value)) AS `客単価`
# MAGIC FROM sales_metrics
# MAGIC WHERE order_month >= '2026-01-01'
# MAGIC GROUP BY order_month
# MAGIC ORDER BY order_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カテゴリごと (2本目のビューに相当)。ビューは作り直していない
# MAGIC SELECT category, MEASURE(total_sales) AS `売上合計`, ROUND(MEASURE(avg_order_value)) AS `客単価`
# MAGIC FROM sales_metrics
# MAGIC GROUP BY category
# MAGIC ORDER BY `売上合計` DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 都道府県ごと (3本目のビューに相当)。定義の中で結合してあるので JOIN を書かない
# MAGIC SELECT prefecture, MEASURE(total_sales) AS `売上合計`, MEASURE(unique_customers) AS `購入顧客数`
# MAGIC FROM sales_metrics
# MAGIC GROUP BY prefecture
# MAGIC ORDER BY `売上合計` DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 会員ランクごと。標準ビューなら、ここで4本目を作っていた
# MAGIC SELECT member_rank, MEASURE(order_count) AS `注文数`, ROUND(MEASURE(avg_order_value)) AS `客単価`
# MAGIC FROM sales_metrics
# MAGIC GROUP BY member_rank
# MAGIC ORDER BY `注文数` DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. の質問にも戻ります。3月の購入顧客数です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 単位を指定しなければ、WHERE で絞った範囲の全体になる。
# MAGIC -- 日次サマリを足したときの335人ではなく、正しい246人が返る。
# MAGIC SELECT
# MAGIC   MEASURE(total_sales)      AS `売上合計`,
# MAGIC   MEASURE(order_count)      AS `注文数`,
# MAGIC   MEASURE(unique_customers) AS `購入顧客数`
# MAGIC FROM sales_metrics
# MAGIC WHERE order_date BETWEEN '2026-03-01' AND '2026-03-31';

# COMMAND ----------

# MAGIC %md
# MAGIC 集計した表を持っていないので、聞かれた単位でそのつど注文データから計算します。だから足し直しが起きません。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- メジャーは、集計する単位が決まらないと値が決まらない。
# MAGIC -- SELECT * は単位を指定していないので、値を作れずエラーになる。
# MAGIC SELECT * FROM sales_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 定義は1か所にある
# MAGIC
# MAGIC - 「メジャー」と「フィールド」の一覧も、YAMLの定義も、カタログエクスプローラーから見られます
# MAGIC - 客単価の計算式はここにしかありません。変えるときはここだけ直します
# MAGIC - Unity Catalogのオブジェクトなので、権限の管理もテーブルと同じです

# COMMAND ----------

def _workspace_host():
    try:
        return spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        return ctx.browserHostName().get()

url = f"https://{_workspace_host()}/explore/data/{CATALOG}/{SCHEMA}/sales_metrics"
displayHTML(f'<a href="{url}" target="_blank" style="font-size:18px">sales_metrics をカタログエクスプローラーで開く</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. SQLを書かない人が使う
# MAGIC
# MAGIC ここからはダッシュボードとGenieの画面です。
# MAGIC
# MAGIC - ダッシュボードに `sales_metrics` を追加し、カテゴリ別の売上を棒グラフにします。`MEASURE()` は書きません
# MAGIC - Genieスペースに `sales_metrics` を追加し、「2026年3月は何人のお客さんが買いましたか」と日本語で聞きます
# MAGIC - 同じ定義は、アラートからも、Power BIやTableauのような外部BIツール、Excelからも参照できます

# COMMAND ----------

# MAGIC %md
# MAGIC ## 後片付け (任意)

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
