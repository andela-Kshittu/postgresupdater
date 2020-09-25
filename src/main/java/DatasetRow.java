    public class DatasetRow {

        private String dataset_iri;
        private String domain_id;
        private String base_rdf;
        private String edit_rdf;

        public DatasetRow(String dataset_iri, String domain_id, String base_rdf, String edit_rdf) {
            this.dataset_iri = dataset_iri;
            this.domain_id = domain_id;
            this.base_rdf = base_rdf;
            this.edit_rdf = edit_rdf;
        }

        public String getDataset_iri() {
            return dataset_iri;
        }

        public String getDomain_id() {
            return domain_id;
        }

        public String getBase_rdf() {
            return base_rdf;
        }

        public String getEdit_rdf() {
            return edit_rdf;
        }

        @Override
        public String toString() {
            return "DatasetRow{" +
                    "dataset_iri='" + dataset_iri + '\'' +
                    '}';
        }
    }