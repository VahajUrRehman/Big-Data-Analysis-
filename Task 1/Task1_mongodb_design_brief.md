# MongoDB Design Brief

### Online Retail II · Document-oriented design using the GitHub project dataset

> **Design decision:** Store one invoice per document with its line items embedded. PostgreSQL remains the authoritative analytical database, while MongoDB provides a flexible invoice-centred read model.

---

## 1. Dataset and design objective

The GitHub project dataset contains 1,033,036 transaction lines representing 53,628 invoices, 5,942 customers, and 5,305 products. The relational implementation separates this information into `customers`, `products`, `invoices`, and `invoice_items`. In MongoDB, the objective is to make complete invoices easy to retrieve without repeatedly joining four collections. An invoice-centred document is therefore the most suitable model because the invoice header and its line items are normally read together.

---

## 2. Proposed document structure

MongoDB should use an `invoices` collection. The invoice number becomes `_id`; invoice-level fields include the date and cancellation status. A customer snapshot stores the customer ID and country, while an embedded `items` array stores the product code, description, quantity, unit price, and calculated line total.

```javascript
{
  _id: "489434",
  invoice_date: ISODate("2009-12-01T07:45:00Z"),
  is_cancelled: false,
  customer: {
    customer_id: 13085,
    country: "United Kingdom"
  },
  items: [
    {
      stock_code: "85048",
      description: "15CM CHRISTMAS GLASS BALL 20 LIGHTS",
      quantity: 12,
      unit_price: 6.95,
      line_total: 83.40
    }
  ]
}
```

This model intentionally duplicates product descriptions and customer countries as historical snapshots. A later change to a product name or customer location will therefore not rewrite an earlier invoice. If an unusually large invoice approached MongoDB's 16 MB document limit, its items could be split into numbered bucket documents, although this is unlikely for this dataset.

---

## 3. Ingestion mapping

The ingestion process should group the GitHub workbook by `Invoice_Clean`. For each group, it should take the earliest `InvoiceDate`, the first customer and country values, and the supplied `is_cancelled` value. Every row in that group becomes one entry in the `items` array. Numeric types should be preserved for quantity and price, dates should be converted to BSON dates, and invoice and stock codes should remain strings because some codes contain letters. Batch inserts or bulk writes should be used for performance. Completion must be verified by comparing the MongoDB document count with the 53,628 distinct invoices in PostgreSQL.

---

## 4. Recommended indexes

```javascript
db.invoices.createIndex({ invoice_date: 1 })
db.invoices.createIndex({ "customer.customer_id": 1 })
db.invoices.createIndex({ "customer.country": 1, invoice_date: 1 })
db.invoices.createIndex({ "items.stock_code": 1 })
db.invoices.createIndex({ is_cancelled: 1, invoice_date: 1 })
```

These indexes support time-based reporting, customer histories, country analysis, product searches, and filtering completed versus cancelled transactions. Indexes should be selected from demonstrated query requirements because every additional index consumes storage and increases write cost.

---

## 5. Example analytical query

The following aggregation calculates completed-sales revenue by country:

```javascript
db.invoices.aggregate([
  { $match: { is_cancelled: false } },
  { $unwind: "$items" },
  { $match: { "items.quantity": { $gt: 0 }, "items.unit_price": { $gt: 0 } } },
  {
    $group: {
      _id: "$customer.country",
      total_revenue: {
        $sum: { $multiply: ["$items.quantity", "$items.unit_price"] }
      },
      completed_invoices: { $addToSet: "$_id" }
    }
  },
  {
    $project: {
      country: "$_id",
      total_revenue: 1,
      completed_invoices: { $size: "$completed_invoices" }
    }
  },
  { $sort: { total_revenue: -1 } }
])
```

---

## 6. MongoDB compared with PostgreSQL

MongoDB is useful for application-facing invoice retrieval because one document contains the complete transaction and can be returned directly as JSON. Its flexible schema also allows new item attributes without immediately changing several relational tables. PostgreSQL is stronger for this project's authoritative analytical workload: foreign keys prevent orphaned records, normalization avoids unnecessary duplication, and SQL joins, common table expressions, and window functions suit cross-invoice financial analysis. MongoDB should therefore complement rather than replace PostgreSQL.

---

## 7. CAP theorem considerations

During a network partition, a distributed database must choose between consistency and availability. A MongoDB replica set can favour consistency and partition tolerance by using majority write concern. If the primary loses contact with a majority of voting members, it stops accepting writes rather than risk conflicting invoice records; a member in the majority can elect a new primary. This may temporarily reduce write availability, but it is appropriate for retail data because contradictory invoices or totals would be more harmful than a short delay. Secondary reads can improve read availability, although they may return stale data depending on the read preference and read concern.

---

## 8. GDPR and security considerations

Only the minimum customer information required for analysis should be stored. Although the dataset uses numeric customer IDs rather than names, those IDs should still be treated as pseudonymous identifiers. Access should follow least-privilege principles, database connections should use authentication and encryption, and backups should be protected. A retention policy should remove information that is no longer required. If a valid erasure request applied to a customer, the identifier would need to be removed or anonymised consistently across all matching invoice documents and derived analytical outputs.
