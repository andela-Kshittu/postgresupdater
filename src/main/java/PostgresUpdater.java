import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PostgresUpdater {

    private static final String DC_TERMS_IDENTIFIER = "http://purl.org/dc/terms/identifier";
    private static final String DC_TERMS_PUBLISHER = "http://purl.org/dc/terms/publisher";
    private static Logger lgr = Logger.getLogger(PostgresUpdater.class.getName());

    public static void main(String[] args) {

        String url = args[0];
        String user = args[1];
        String password = args[2];
        int fetchSize = Integer.parseInt(args[3]);

        fetchRecords(url, user, password, fetchSize);
    }

    // Fetch and update records in batches of specified fetchSize
    private static void fetchRecords(String url, String user, String password, int fetchSize) {
        try (Connection con = DriverManager.getConnection(url, user, password)) {
            // make sure autocommit is off
            con.setAutoCommit(false);
            PreparedStatement pst = con.prepareStatement("SELECT * FROM dwec_dataset");

            // Turn use of the cursor on.
            pst.setFetchSize(fetchSize);
            ResultSet rs = pst.executeQuery();

            List<DatasetRow> datasetRows = new ArrayList<>();

            while (rs.next()) {
                String dataset_iri = rs.getString(1);
                String domain_id = rs.getString(2);
                String base_rdf = rs.getString(7);
                String edit_rdf = rs.getString(8);

                DatasetRow dr = new DatasetRow(dataset_iri, domain_id, base_rdf, edit_rdf);
                datasetRows.add(dr);

                if (datasetRows.size() == fetchSize) {
                    lgr.log(Level.INFO,"### updating a batch  of dataset rows : " + datasetRows);
                    updateRowsInBatches(datasetRows, url, user, password);

                    datasetRows.clear();
                }
            }

            if (!datasetRows.isEmpty()) {
                lgr.log(Level.INFO,"### updating remaining batch of dataset rows : " + datasetRows);
                updateRowsInBatches(datasetRows, url, user, password);

                datasetRows.clear();
            }

            rs.close();
            // Close the statement.
            pst.close();
        } catch (SQLException ex) {
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    private static void updateRowsInBatches(List<DatasetRow> drList, String url, String user, String password) {
        String query = "UPDATE dwec_dataset SET edit_rdf = ?::json, base_rdf = ?::json WHERE dataset_iri = ? " +
                "AND domain_id = ?";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement pst = conn.prepareStatement(query)) {

            conn.setAutoCommit(false);

            for (DatasetRow dr : drList) {
                try {
                    String dataset_iri = dr.getDataset_iri();
                    String domain_id = dr.getDomain_id();
                    String base_rdf = dr.getBase_rdf();
                    String edit_rdf = dr.getEdit_rdf();

                    JSONObject publisherObject = new JSONObject();
                    publisherObject.put("type", "literal");
                    publisherObject.put("value", domain_id.split(":")[1]);

                    JSONArray publisherArray = new JSONArray();
                    publisherArray.put(publisherObject);

                    JSONObject editRdfObject = null;
                    if (edit_rdf != null && !edit_rdf.isEmpty()) {
                        // convert edit_rdf json string to json object
                        editRdfObject = new JSONObject(edit_rdf);
                    }

                    JSONObject baseRdfObject = null;
                    if (base_rdf != null && !base_rdf.isEmpty()) {
                        // convert edit_rdf json string to json object
                        baseRdfObject = new JSONObject(base_rdf);
                    }

                    JSONObject editRdfDatasetObject = null;
                    if (editRdfObject != null && editRdfObject.has(dataset_iri)) {

                        editRdfDatasetObject = editRdfObject.getJSONObject(dataset_iri);

                        if (editRdfDatasetObject.has(DC_TERMS_IDENTIFIER)) {
                            editRdfDatasetObject.put(DC_TERMS_PUBLISHER, publisherArray);
                            editRdfObject.put(dataset_iri, editRdfDatasetObject);

                            lgr.log(Level.INFO,"#### updated edit_rdf for : " + dataset_iri);
                        }
                    }

                    JSONObject baseRdfDatasetObject = null;
                    if (baseRdfObject != null && baseRdfObject.has(dataset_iri)) {

                        baseRdfDatasetObject = baseRdfObject.getJSONObject(dataset_iri);

                        if (baseRdfDatasetObject.has(DC_TERMS_IDENTIFIER)) {
                            baseRdfDatasetObject.put(DC_TERMS_PUBLISHER, publisherArray);
                            baseRdfObject.put(dataset_iri, baseRdfDatasetObject);

                            lgr.log(Level.INFO,"#### updated base_rdf for : " + dataset_iri);
                        }
                    }

                    pst.setObject(1, editRdfDatasetObject == null ? null : editRdfObject.toString());
                    pst.setObject(2, baseRdfDatasetObject == null ? null : baseRdfObject.toString());
                    pst.setString(3, dataset_iri);
                    pst.setString(4, domain_id);
                    pst.addBatch();
                } catch (Exception ex) {
                    lgr.log(Level.SEVERE, ex.getMessage(), ex);
                }
            }

            int[] updateCounts = pst.executeBatch();
            lgr.log(Level.INFO,"#### batch  update total rows count : " + updateCounts.length);
            conn.commit();
            conn.setAutoCommit(true);
        } catch (Exception ex) {
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
}