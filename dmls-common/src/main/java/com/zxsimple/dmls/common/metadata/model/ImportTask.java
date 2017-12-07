package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;

/**
 * Created by zxsimple on 4-13.
 */

@Entity
@Table(name = "import_task")
public class ImportTask {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "import_flow_id")
    private Long importFlowId;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "data_source_id")
    private DataSource dataSource;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "imported_dataset_id")
    private ImportedDataset importedDataset;

    @Column(name="is_deleted")
    private int isDeleted;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public ImportedDataset getImportedDataset() {
        return importedDataset;
    }

    public void setImportedDataset(ImportedDataset importedDataset) {
        this.importedDataset = importedDataset;
    }

    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }

    public Long getImportFlowId() {
        return importFlowId;
    }

    public void setImportFlowId(Long importFlowId) {
        this.importFlowId = importFlowId;
    }
}
