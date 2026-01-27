⏺ Unified Deletion Vector Framework - Design Document

Executive Summary

This document proposes a unified Deletion Vector (DV) framework for Apache Iceberg that treats deletion vectors as a storage optimization rather than a distinct feature. The framework supports both position and equality deletes with
a single, extensible architecture.

Key Principles:
- Simplicity: Single abstraction for all delete types
- Transparency: Automatic format selection, no user configuration
- Extensibility: Designed for future type support without breaking changes
- Compatibility: Backward compatible with existing delete files

  ---                                                                                                                                                                                                                                     
Table of Contents

1. #specification-changes
2. #architecture-overview
3. #api-design
4. #automatic-format-selection
5. #implementation-strategy
6. #migration-and-compatibility
7. #extension-points
8. #testing-strategy

  ---                                                                                                                                                                                                                                     
1. Specification Changes

1.1 New Delete Encoding Field

Add an explicit encoding field to the DeleteFile metadata structure:

// Existing DeleteFile interface                                                                                                                                                                                                        
public interface DeleteFile extends ContentFile<DeleteFile> {                                                                                                                                                                           
FileContent content();  // POSITION_DELETES or EQUALITY_DELETES                                                                                                                                                                       
FileFormat format();    // PARQUET, AVRO, ORC, or PUFFIN

    // NEW: Explicit encoding field                                                                                                                                                                                                       
    DeleteEncoding encoding();  // How deletes are physically stored                                                                                                                                                                      
                                                                                                                                                                                                                                          
    List<Integer> equalityFieldIds();  // For equality deletes                                                                                                                                                                            
    CharSequence referencedDataFile(); // For position deletes (DV only)                                                                                                                                                                  
}

1.2 Delete Encoding Enum

public enum DeleteEncoding {                                                                                                                                                                                                            
/**                                                                                                                                                                                                                                   
* Full row storage (default for traditional delete files).                                                                                                                                                                           
* - For position deletes: stores (file_path, position) tuples                                                                                                                                                                        
* - For equality deletes: stores full row data matching equality fields                                                                                                                                                              
*/                                                                                                                                                                                                                                   
FULL_ROW(0),

    /**                                                                                                                                                                                                                                   
     * Deletion Vector storage using compressed bitmaps.                                                                                                                                                                                  
     * - For position deletes: Roaring bitmap of row positions                                                                                                                                                                            
     * - For equality deletes: Roaring bitmap of field values (primitives only)                                                                                                                                                           
     *                                                                                                                                                                                                                                    
     * Requirements:                                                                                                                                                                                                                      
     * - Format must be PUFFIN                                                                                                                                                                                                            
     * - For equality deletes: single primitive field (LONG in v1)                                                                                                                                                                        
     */                                                                                                                                                                                                                                   
    DELETION_VECTOR(1),                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
    /**                                                                                                                                                                                                                                   
     * Reserved for future use: hash-based encoding for composite keys.                                                                                                                                                                   
     */                                                                                                                                                                                                                                   
    HASH_MAP(2);                                                                                                                                                                                                                          
                                                                                                                                                                                                                                          
    private final int id;                                                                                                                                                                                                                 
                                                                                                                                                                                                                                          
    DeleteEncoding(int id) {                                                                                                                                                                                                              
      this.id = id;                                                                                                                                                                                                                       
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    public int id() {                                                                                                                                                                                                                     
      return id;                                                                                                                                                                                                                          
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    public static DeleteEncoding fromId(int id) {                                                                                                                                                                                         
      switch (id) {                                                                                                                                                                                                                       
        case 0: return FULL_ROW;                                                                                                                                                                                                          
        case 1: return DELETION_VECTOR;                                                                                                                                                                                                   
        case 2: return HASH_MAP;                                                                                                                                                                                                          
        default:                                                                                                                                                                                                                          
          throw new IllegalArgumentException("Unknown encoding id: " + id);                                                                                                                                                               
      }                                                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
}

1.3 Metadata Encoding

Avro Schema Changes:

{                                                                                                                                                                                                                                       
"type": "record",                                                                                                                                                                                                                     
"name": "DeleteFile",                                                                                                                                                                                                                 
"fields": [                                                                                                                                                                                                                           
{"name": "content", "type": "int"},                                                                                                                                                                                                 
{"name": "file_path", "type": "string"},                                                                                                                                                                                            
{"name": "file_format", "type": "string"},                                                                                                                                                                                          
{"name": "encoding", "type": "int", "default": 0},  // NEW FIELD                                                                                                                                                                    
{"name": "partition", "type": ["null", "PartitionData"], "default": null},                                                                                                                                                          
{"name": "record_count", "type": "long"},                                                                                                                                                                                           
{"name": "file_size_in_bytes", "type": "long"},                                                                                                                                                                                     
{"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "default": null},                                                                                                                                     
{"name": "referenced_data_file", "type": ["null", "string"], "default": null}                                                                                                                                                       
]                                                                                                                                                                                                                                     
}

Default Value: encoding = 0 (FULL_ROW) ensures backward compatibility.

1.4 Validation Rules

public class DeleteFileValidator {                                                                                                                                                                                                      
public static void validate(DeleteFile deleteFile) {                                                                                                                                                                                  
DeleteEncoding encoding = deleteFile.encoding();                                                                                                                                                                                    
FileContent content = deleteFile.content();                                                                                                                                                                                         
FileFormat format = deleteFile.format();

      // Rule 1: DELETION_VECTOR requires PUFFIN format                                                                                                                                                                                   
      if (encoding == DeleteEncoding.DELETION_VECTOR) {                                                                                                                                                                                   
        Preconditions.checkArgument(                                                                                                                                                                                                      
            format == FileFormat.PUFFIN,                                                                                                                                                                                                  
            "Deletion vector encoding requires PUFFIN format, got: %s", format);                                                                                                                                                          
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Rule 2: Equality DV requires single primitive field                                                                                                                                                                              
      if (encoding == DeleteEncoding.DELETION_VECTOR &&                                                                                                                                                                                   
          content == FileContent.EQUALITY_DELETES) {                                                                                                                                                                                      
        List<Integer> eqIds = deleteFile.equalityFieldIds();                                                                                                                                                                              
        Preconditions.checkArgument(                                                                                                                                                                                                      
            eqIds != null && eqIds.size() == 1,                                                                                                                                                                                           
            "Equality deletion vector requires exactly one field, got: %s",                                                                                                                                                               
            eqIds == null ? 0 : eqIds.size());                                                                                                                                                                                            
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Rule 3: Position DV requires referenced data file                                                                                                                                                                                
      if (encoding == DeleteEncoding.DELETION_VECTOR &&                                                                                                                                                                                   
          content == FileContent.POSITION_DELETES) {                                                                                                                                                                                      
        Preconditions.checkArgument(                                                                                                                                                                                                      
            deleteFile.referencedDataFile() != null,                                                                                                                                                                                      
            "Position deletion vector requires referenced data file");                                                                                                                                                                    
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Rule 4: FULL_ROW can use any format                                                                                                                                                                                              
      if (encoding == DeleteEncoding.FULL_ROW) {                                                                                                                                                                                          
        Preconditions.checkArgument(                                                                                                                                                                                                      
            format == FileFormat.PARQUET ||                                                                                                                                                                                               
            format == FileFormat.AVRO ||                                                                                                                                                                                                  
            format == FileFormat.ORC,                                                                                                                                                                                                     
            "Full row encoding requires PARQUET, AVRO, or ORC format");                                                                                                                                                                   
      }                                                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
}
                                                                                                                                                                                                                                          
---                                                                                                                                                                                                                                     
2. Architecture Overview

2.1 Layered Design

┌─────────────────────────────────────────────────────────┐                                                                                                                                                                             
│  User API Layer                                         │                                                                                                                                                                             
│  - RowDelta.addDeletes(DeleteFile)                     │                                                                                                                                                                              
│  - No awareness of encoding                             │                                                                                                                                                                             
└─────────────────────────────────────────────────────────┘                                                                                                                                                                             
│                                                                                                                                                                                                              
▼                                                                                                                                                                                                              
┌─────────────────────────────────────────────────────────┐                                                                                                                                                                             
│  Delete Writer Selection Layer                          │                                                                                                                                                                             
│  - Analyzes delete characteristics                      │                                                                                                                                                                             
│  - Chooses optimal encoding automatically               │                                                                                                                                                                             
│  - Factory: createDeleteWriter(...)                     │                                                                                                                                                                             
└─────────────────────────────────────────────────────────┘                                                                                                                                                                             
│                                                                                                                                                                                                              
▼                                                                                                                                                                                                              
┌─────────────────────────────────────────────────────────┐                                                                                                                                                                             
│  Encoding-Specific Writers                              │                                                                                                                                                                             
│  - FullRowDeleteWriter (PARQUET/AVRO/ORC)              │                                                                                                                                                                              
│  - DeletionVectorWriter (PUFFIN)                       │                                                                                                                                                                              
└─────────────────────────────────────────────────────────┘                                                                                                                                                                             
│                                                                                                                                                                                                              
▼                                                                                                                                                                                                              
┌─────────────────────────────────────────────────────────┐                                                                                                                                                                             
│  Storage Layer                                          │                                                                                                                                                                             
│  - PUFFIN: Roaring bitmap blobs                        │                                                                                                                                                                              
│  - PARQUET/AVRO/ORC: Row-based storage                 │                                                                                                                                                                              
└─────────────────────────────────────────────────────────┘

                      READING PATH                                                                                                                                                                                                        

┌─────────────────────────────────────────────────────────┐                                                                                                                                                                             
│  DeleteFilter (existing)                                │                                                                                                                                                                             
│  - Unified interface for all delete types              │                                                                                                                                                                              
└─────────────────────────────────────────────────────────┘                                                                                                                                                                             
│                                                                                                                                                                                                              
▼                                                                                                                                                                                                              
┌─────────────────────────────────────────────────────────┐                                                                                                                                                                             
│  DeleteLoader (enhanced)                                │                                                                                                                                                                             
│  - Detects encoding from metadata                       │                                                                                                                                                                             
│  - Delegates to appropriate loader                      │                                                                                                                                                                             
└─────────────────────────────────────────────────────────┘                                                                                                                                                                             
│                                                                                                                                                                                                              
▼                                                                                                                                                                                                              
┌─────────────────────────────────────────────────────────┐                                                                                                                                                                             
│  Unified Delete Set Abstraction                        │                                                                                                                                                                              
│  - PositionDeleteIndex (for position deletes)          │                                                                                                                                                                              
│  - StructLikeSet (for equality deletes)                │                                                                                                                                                                              
│  - Both support FULL_ROW and DV encodings              │                                                                                                                                                                              
└─────────────────────────────────────────────────────────┘

2.2 Core Abstraction: Delete Set

The key insight is to provide a unified interface regardless of encoding:

/**
* Abstraction for a set of deleted positions.
* Implementation may use bitmaps or traditional storage.                                                                                                                                                                               
  */                                                                                                                                                                                                                                     
  public interface PositionDeleteIndex {                                                                                                                                                                                                  
  boolean isDeleted(long position);                                                                                                                                                                                                     
  long cardinality();

    // Factory methods                                                                                                                                                                                                                    
    static PositionDeleteIndex fromDeletionVector(RoaringPositionBitmap bitmap) {                                                                                                                                                         
      return new BitmapPositionIndex(bitmap);                                                                                                                                                                                             
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    static PositionDeleteIndex fromRows(Iterable<Record> positions) {                                                                                                                                                                     
      return new RowBasedPositionIndex(positions);                                                                                                                                                                                        
    }                                                                                                                                                                                                                                     
}

/**
* Abstraction for a set of deleted values (equality deletes).
* Implementation may use bitmaps or traditional storage.                                                                                                                                                                               
  */                                                                                                                                                                                                                                     
  public interface EqualityDeleteSet extends Set<StructLike> {                                                                                                                                                                            
  // Standard Set interface

    // Factory methods                                                                                                                                                                                                                    
    static EqualityDeleteSet fromDeletionVector(                                                                                                                                                                                          
        RoaringPositionBitmap bitmap,                                                                                                                                                                                                     
        int fieldId,                                                                                                                                                                                                                      
        Schema schema) {                                                                                                                                                                                                                  
      return new BitmapBackedStructLikeSet(bitmap, fieldId, schema);                                                                                                                                                                      
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    static EqualityDeleteSet fromRows(                                                                                                                                                                                                    
        Iterable<Record> rows,                                                                                                                                                                                                            
        Schema schema) {                                                                                                                                                                                                                  
      return new StructLikeSet(rows, schema);                                                                                                                                                                                             
    }                                                                                                                                                                                                                                     
}
                                                                                                                                                                                                                                          
---                                                                                                                                                                                                                                     
3. API Design

3.1 Writer API (Automatic Selection)

/**
* Factory for creating delete writers with automatic encoding selection.                                                                                                                                                               
  */                                                                                                                                                                                                                                     
  public class DeleteWriterFactory {                                                                                                                                                                                                      
  private final Table table;                                                                                                                                                                                                            
  private final OutputFileFactory fileFactory;

    /**                                                                                                                                                                                                                                   
     * Creates a position delete writer.                                                                                                                                                                                                  
     * Automatically chooses between FULL_ROW and DELETION_VECTOR encoding.                                                                                                                                                               
     */                                                                                                                                                                                                                                   
    public PositionDeleteWriter<T> createPositionWriter(                                                                                                                                                                                  
        DataFile dataFile,                                                                                                                                                                                                                
        DeleteGranularity granularity) {                                                                                                                                                                                                  
                                                                                                                                                                                                                                          
      // Analyze characteristics                                                                                                                                                                                                          
      boolean useDV = shouldUseDeletionVector(                                                                                                                                                                                            
          table.properties(),                                                                                                                                                                                                             
          granularity,                                                                                                                                                                                                                    
          FileContent.POSITION_DELETES);                                                                                                                                                                                                  
                                                                                                                                                                                                                                          
      if (useDV) {                                                                                                                                                                                                                        
        return new DeletionVectorPositionWriter<>(                                                                                                                                                                                        
            fileFactory,                                                                                                                                                                                                                  
            dataFile,                                                                                                                                                                                                                     
            granularity);                                                                                                                                                                                                                 
      } else {                                                                                                                                                                                                                            
        return new FullRowPositionWriter<>(                                                                                                                                                                                               
            fileFactory,                                                                                                                                                                                                                  
            dataFile);                                                                                                                                                                                                                    
      }                                                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    /**                                                                                                                                                                                                                                   
     * Creates an equality delete writer.                                                                                                                                                                                                 
     * Automatically chooses between FULL_ROW and DELETION_VECTOR encoding.                                                                                                                                                               
     */                                                                                                                                                                                                                                   
    public EqualityDeleteWriter<T> createEqualityWriter(                                                                                                                                                                                  
        List<Integer> equalityFieldIds,                                                                                                                                                                                                   
        Schema deleteSchema) {                                                                                                                                                                                                            
                                                                                                                                                                                                                                          
      // Check if DV is applicable                                                                                                                                                                                                        
      boolean canUseDV = equalityFieldIds.size() == 1 &&                                                                                                                                                                                  
          isPrimitiveType(deleteSchema, equalityFieldIds.get(0));                                                                                                                                                                         
                                                                                                                                                                                                                                          
      boolean shouldUseDV = canUseDV && shouldUseDeletionVector(                                                                                                                                                                          
          table.properties(),                                                                                                                                                                                                             
          DeleteGranularity.FILE,  // Equality deletes are always file-level                                                                                                                                                              
          FileContent.EQUALITY_DELETES);                                                                                                                                                                                                  
                                                                                                                                                                                                                                          
      if (shouldUseDV) {                                                                                                                                                                                                                  
        return new DeletionVectorEqualityWriter<>(                                                                                                                                                                                        
            fileFactory,                                                                                                                                                                                                                  
            equalityFieldIds.get(0),                                                                                                                                                                                                      
            deleteSchema);                                                                                                                                                                                                                
      } else {                                                                                                                                                                                                                            
        return new FullRowEqualityWriter<>(                                                                                                                                                                                               
            fileFactory,                                                                                                                                                                                                                  
            equalityFieldIds,                                                                                                                                                                                                             
            deleteSchema);                                                                                                                                                                                                                
      }                                                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    /**                                                                                                                                                                                                                                   
     * Decision logic for using deletion vectors.                                                                                                                                                                                         
     */                                                                                                                                                                                                                                   
    private boolean shouldUseDeletionVector(                                                                                                                                                                                              
        Map<String, String> tableProperties,                                                                                                                                                                                              
        DeleteGranularity granularity,                                                                                                                                                                                                    
        FileContent content) {                                                                                                                                                                                                            
                                                                                                                                                                                                                                          
      // Check table-level hint                                                                                                                                                                                                           
      String hint = tableProperties.get(TableProperties.DELETE_ENCODING_HINT);                                                                                                                                                            
      if ("full-row".equalsIgnoreCase(hint)) {                                                                                                                                                                                            
        return false;  // User explicitly disabled DVs                                                                                                                                                                                    
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // For position deletes, prefer DV for file-level granularity                                                                                                                                                                       
      if (content == FileContent.POSITION_DELETES) {                                                                                                                                                                                      
        return granularity == DeleteGranularity.FILE;                                                                                                                                                                                     
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // For equality deletes, always prefer DV when applicable                                                                                                                                                                           
      // (it's almost always more efficient for primitives)                                                                                                                                                                               
      return true;                                                                                                                                                                                                                        
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    private boolean isPrimitiveType(Schema schema, int fieldId) {                                                                                                                                                                         
      Type type = schema.findType(fieldId);                                                                                                                                                                                               
      return type != null && type.isPrimitiveType() &&                                                                                                                                                                                    
             type.typeId() == Type.TypeID.LONG;  // v1: only LONG                                                                                                                                                                         
    }                                                                                                                                                                                                                                     
}

3.2 New Table Property (Hint Only)

public class TableProperties {                                                                                                                                                                                                          
/**                                                                                                                                                                                                                                   
* Hint for delete encoding preference. Not a strict requirement.                                                                                                                                                                     
*
* Supported values:                                                                                                                                                                                                                  
* - "auto" (default): automatically select best encoding                                                                                                                                                                             
* - "deletion-vector": prefer deletion vectors when applicable                                                                                                                                                                       
* - "full-row": prefer traditional full-row storage                                                                                                                                                                                  
*
* Writers may ignore this hint based on data characteristics.                                                                                                                                                                        
*/                                                                                                                                                                                                                                   
public static final String DELETE_ENCODING_HINT = "write.delete.encoding-hint";                                                                                                                                                       
public static final String DELETE_ENCODING_HINT_DEFAULT = "auto";                                                                                                                                                                     
}

Key difference from current design: This is a hint, not a strict control. Writers have final say.

3.3 Reader API (Transparent Loading)

/**
* Enhanced DeleteLoader that handles all encodings transparently.                                                                                                                                                                      
  */                                                                                                                                                                                                                                     
  public class BaseDeleteLoader implements DeleteLoader {

    @Override                                                                                                                                                                                                                             
    public PositionDeleteIndex loadPositionDeletes(                                                                                                                                                                                       
        Iterable<DeleteFile> deleteFiles,                                                                                                                                                                                                 
        CharSequence filePath) {                                                                                                                                                                                                          
                                                                                                                                                                                                                                          
      // Separate by encoding                                                                                                                                                                                                             
      List<DeleteFile> dvFiles = new ArrayList<>();                                                                                                                                                                                       
      List<DeleteFile> fullRowFiles = new ArrayList<>();                                                                                                                                                                                  
                                                                                                                                                                                                                                          
      for (DeleteFile file : deleteFiles) {                                                                                                                                                                                               
        if (file.encoding() == DeleteEncoding.DELETION_VECTOR) {                                                                                                                                                                          
          dvFiles.add(file);                                                                                                                                                                                                              
        } else {                                                                                                                                                                                                                          
          fullRowFiles.add(file);                                                                                                                                                                                                         
        }                                                                                                                                                                                                                                 
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Load each type                                                                                                                                                                                                                   
      PositionDeleteIndex dvIndex = loadDeletionVectors(dvFiles);                                                                                                                                                                         
      PositionDeleteIndex fullRowIndex = loadFullRowPositions(fullRowFiles, filePath);                                                                                                                                                    
                                                                                                                                                                                                                                          
      // Merge if needed                                                                                                                                                                                                                  
      if (dvIndex != null && fullRowIndex != null) {                                                                                                                                                                                      
        return PositionDeleteIndex.merge(dvIndex, fullRowIndex);                                                                                                                                                                          
      }                                                                                                                                                                                                                                   
      return dvIndex != null ? dvIndex : fullRowIndex;                                                                                                                                                                                    
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Override                                                                                                                                                                                                                             
    public Set<StructLike> loadEqualityDeletes(                                                                                                                                                                                           
        Iterable<DeleteFile> deleteFiles,                                                                                                                                                                                                 
        Schema projection) {                                                                                                                                                                                                              
                                                                                                                                                                                                                                          
      // Separate by encoding                                                                                                                                                                                                             
      List<DeleteFile> dvFiles = new ArrayList<>();                                                                                                                                                                                       
      List<DeleteFile> fullRowFiles = new ArrayList<>();                                                                                                                                                                                  
                                                                                                                                                                                                                                          
      for (DeleteFile file : deleteFiles) {                                                                                                                                                                                               
        if (file.encoding() == DeleteEncoding.DELETION_VECTOR) {                                                                                                                                                                          
          dvFiles.add(file);                                                                                                                                                                                                              
        } else {                                                                                                                                                                                                                          
          fullRowFiles.add(file);                                                                                                                                                                                                         
        }                                                                                                                                                                                                                                 
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Load DV files                                                                                                                                                                                                                    
      Set<StructLike> result = StructLikeSet.create(projection.asStruct());                                                                                                                                                               
                                                                                                                                                                                                                                          
      if (!dvFiles.isEmpty()) {                                                                                                                                                                                                           
        for (DeleteFile file : dvFiles) {                                                                                                                                                                                                 
          Set<StructLike> dvSet = readDeletionVectorEquality(file, projection);                                                                                                                                                           
          result.addAll(dvSet);                                                                                                                                                                                                           
        }                                                                                                                                                                                                                                 
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Load full-row files                                                                                                                                                                                                              
      if (!fullRowFiles.isEmpty()) {                                                                                                                                                                                                      
        Iterable<StructLike> fullRows = loadFullRowEquality(fullRowFiles, projection);                                                                                                                                                    
        Iterables.addAll(result, fullRows);                                                                                                                                                                                               
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      return result;                                                                                                                                                                                                                      
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    private Set<StructLike> readDeletionVectorEquality(                                                                                                                                                                                   
        DeleteFile file,                                                                                                                                                                                                                  
        Schema projection) {                                                                                                                                                                                                              
                                                                                                                                                                                                                                          
      Preconditions.checkArgument(                                                                                                                                                                                                        
          file.encoding() == DeleteEncoding.DELETION_VECTOR,                                                                                                                                                                              
          "Expected deletion vector encoding");                                                                                                                                                                                           
                                                                                                                                                                                                                                          
      int fieldId = file.equalityFieldIds().get(0);                                                                                                                                                                                       
      InputFile input = loadInputFile.apply(file);                                                                                                                                                                                        
      RoaringPositionBitmap bitmap = readBitmapFromPuffin(input);                                                                                                                                                                         
                                                                                                                                                                                                                                          
      return new BitmapBackedStructLikeSet(bitmap, fieldId, projection);                                                                                                                                                                  
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    private PositionDeleteIndex loadDeletionVectors(                                                                                                                                                                                      
        List<DeleteFile> dvFiles) {                                                                                                                                                                                                       
                                                                                                                                                                                                                                          
      if (dvFiles.isEmpty()) {                                                                                                                                                                                                            
        return null;                                                                                                                                                                                                                      
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // For position DVs, we can have multiple files (one per data file)                                                                                                                                                                 
      List<PositionDeleteIndex> indexes = new ArrayList<>();                                                                                                                                                                              
                                                                                                                                                                                                                                          
      for (DeleteFile file : dvFiles) {                                                                                                                                                                                                   
        InputFile input = loadInputFile.apply(file);                                                                                                                                                                                      
        RoaringPositionBitmap bitmap = readBitmapFromPuffin(input);                                                                                                                                                                       
        indexes.add(PositionDeleteIndex.fromBitmap(bitmap, file));                                                                                                                                                                        
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      return PositionDeleteIndex.merge(indexes);                                                                                                                                                                                          
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    private RoaringPositionBitmap readBitmapFromPuffin(InputFile input) {                                                                                                                                                                 
      // Existing implementation - no change                                                                                                                                                                                              
      return EqualityDeleteVectors.readEqualityDeleteVectorBitmap(input);                                                                                                                                                                 
    }                                                                                                                                                                                                                                     
}
                                                                                                                                                                                                                                          
---                                                                                                                                                                                                                                     
4. Automatic Format Selection

4.1 Decision Matrix                                                                                                                                                                                                                     
┌───────────────────────────────────────────────┬─────────────────┬──────────────────────────────────────────┐                                                                                                                          
│                   Scenario                    │    Encoding     │                Rationale                 │                                                                                                                          
├───────────────────────────────────────────────┼─────────────────┼──────────────────────────────────────────┤                                                                                                                          
│ Position deletes, file-level granularity      │ DELETION_VECTOR │ Optimal - single bitmap per data file    │                                                                                                                          
├───────────────────────────────────────────────┼─────────────────┼──────────────────────────────────────────┤                                                                                                                          
│ Position deletes, partition-level granularity │ FULL_ROW        │ DV less efficient for cross-file deletes │                                                                                                                          
├───────────────────────────────────────────────┼─────────────────┼──────────────────────────────────────────┤                                                                                                                          
│ Equality delete, single LONG field            │ DELETION_VECTOR │ Highly compressed bitmap representation  │                                                                                                                          
├───────────────────────────────────────────────┼─────────────────┼──────────────────────────────────────────┤                                                                                                                          
│ Equality delete, multiple fields              │ FULL_ROW        │ DV not yet supported (future: HASH_MAP)  │                                                                                                                          
├───────────────────────────────────────────────┼─────────────────┼──────────────────────────────────────────┤                                                                                                                          
│ Equality delete, non-LONG type                │ FULL_ROW        │ DV not yet supported (extensible)        │                                                                                                                          
├───────────────────────────────────────────────┼─────────────────┼──────────────────────────────────────────┤                                                                                                                          
│ Table property = "full-row"                   │ FULL_ROW        │ User override                            │                                                                                                                          
└───────────────────────────────────────────────┴─────────────────┴──────────────────────────────────────────┘                                                                                                                          
4.2 Cost Model

public class EncodingCostEstimator {

    /**                                                                                                                                                                                                                                   
     * Estimates storage cost for different encodings.                                                                                                                                                                                    
     * Returns the encoding with lowest estimated cost.                                                                                                                                                                                   
     */                                                                                                                                                                                                                                   
    public DeleteEncoding chooseEncoding(                                                                                                                                                                                                 
        FileContent content,                                                                                                                                                                                                              
        long estimatedDeleteCount,                                                                                                                                                                                                        
        long totalRowCount,                                                                                                                                                                                                               
        Schema schema,                                                                                                                                                                                                                    
        List<Integer> equalityFieldIds) {                                                                                                                                                                                                 
                                                                                                                                                                                                                                          
      // Calculate selectivity                                                                                                                                                                                                            
      double selectivity = (double) estimatedDeleteCount / totalRowCount;                                                                                                                                                                 
                                                                                                                                                                                                                                          
      // Full row cost                                                                                                                                                                                                                    
      long fullRowCost = estimateFullRowCost(                                                                                                                                                                                             
          estimatedDeleteCount,                                                                                                                                                                                                           
          schema,                                                                                                                                                                                                                         
          equalityFieldIds);                                                                                                                                                                                                              
                                                                                                                                                                                                                                          
      // Deletion vector cost (if applicable)                                                                                                                                                                                             
      Long dvCost = estimateDeletionVectorCost(                                                                                                                                                                                           
          content,                                                                                                                                                                                                                        
          estimatedDeleteCount,                                                                                                                                                                                                           
          equalityFieldIds,                                                                                                                                                                                                               
          schema);                                                                                                                                                                                                                        
                                                                                                                                                                                                                                          
      if (dvCost == null) {                                                                                                                                                                                                               
        return DeleteEncoding.FULL_ROW;  // DV not applicable                                                                                                                                                                             
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Choose based on cost + selectivity threshold                                                                                                                                                                                     
      // DV is better for sparse deletes (< 50% selectivity)                                                                                                                                                                              
      if (selectivity < 0.5 && dvCost < fullRowCost) {                                                                                                                                                                                    
        return DeleteEncoding.DELETION_VECTOR;                                                                                                                                                                                            
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      return DeleteEncoding.FULL_ROW;                                                                                                                                                                                                     
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    private long estimateFullRowCost(                                                                                                                                                                                                     
        long deleteCount,                                                                                                                                                                                                                 
        Schema schema,                                                                                                                                                                                                                    
        List<Integer> equalityFieldIds) {                                                                                                                                                                                                 
                                                                                                                                                                                                                                          
      // Estimate based on field sizes                                                                                                                                                                                                    
      int avgRowSize = 0;                                                                                                                                                                                                                 
      if (equalityFieldIds != null) {                                                                                                                                                                                                     
        for (int fieldId : equalityFieldIds) {                                                                                                                                                                                            
          avgRowSize += estimateFieldSize(schema.findField(fieldId));                                                                                                                                                                     
        }                                                                                                                                                                                                                                 
      } else {                                                                                                                                                                                                                            
        // Position deletes: (file_path + position)                                                                                                                                                                                       
        avgRowSize = 256 + 8;  // ~256 bytes for path, 8 for long                                                                                                                                                                         
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Parquet compression ~3x                                                                                                                                                                                                          
      return deleteCount * avgRowSize / 3;                                                                                                                                                                                                
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    private Long estimateDeletionVectorCost(                                                                                                                                                                                              
        FileContent content,                                                                                                                                                                                                              
        long deleteCount,                                                                                                                                                                                                                 
        List<Integer> equalityFieldIds,                                                                                                                                                                                                   
        Schema schema) {                                                                                                                                                                                                                  
                                                                                                                                                                                                                                          
      // Check applicability                                                                                                                                                                                                              
      if (content == FileContent.EQUALITY_DELETES) {                                                                                                                                                                                      
        if (equalityFieldIds == null || equalityFieldIds.size() != 1) {                                                                                                                                                                   
          return null;  // Not applicable                                                                                                                                                                                                 
        }                                                                                                                                                                                                                                 
                                                                                                                                                                                                                                          
        Type type = schema.findType(equalityFieldIds.get(0));                                                                                                                                                                             
        if (type == null || type.typeId() != Type.TypeID.LONG) {                                                                                                                                                                          
          return null;  // Only LONG supported in v1                                                                                                                                                                                      
        }                                                                                                                                                                                                                                 
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Roaring bitmap cost estimation                                                                                                                                                                                                   
      // Worst case: ~2 bytes per value                                                                                                                                                                                                   
      // Best case: ~0.1 bytes per value (large runs)                                                                                                                                                                                     
      // Use conservative estimate: 1 byte per value                                                                                                                                                                                      
      return deleteCount + 1024;  // +1KB overhead for PUFFIN wrapper                                                                                                                                                                     
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    private int estimateFieldSize(Types.NestedField field) {                                                                                                                                                                              
      switch (field.type().typeId()) {                                                                                                                                                                                                    
        case INTEGER: return 4;                                                                                                                                                                                                           
        case LONG: return 8;                                                                                                                                                                                                              
        case FLOAT: return 4;                                                                                                                                                                                                             
        case DOUBLE: return 8;                                                                                                                                                                                                            
        case DATE: return 4;                                                                                                                                                                                                              
        case TIMESTAMP: return 8;                                                                                                                                                                                                         
        case STRING: return 64;  // Average estimate                                                                                                                                                                                      
        case BINARY: return 128; // Average estimate                                                                                                                                                                                      
        default: return 32;                                                                                                                                                                                                               
      }                                                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
}

4.3 Override Mechanism

Users can override automatic selection:

-- Disable deletion vectors for this table                                                                                                                                                                                              
ALTER TABLE my_table SET TBLPROPERTIES (                                                                                                                                                                                                
'write.delete.encoding-hint' = 'full-row'                                                                                                                                                                                             
);

-- Prefer deletion vectors when applicable                                                                                                                                                                                              
ALTER TABLE my_table SET TBLPROPERTIES (                                                                                                                                                                                                
'write.delete.encoding-hint' = 'deletion-vector'                                                                                                                                                                                      
);

-- Automatic selection (default)                                                                                                                                                                                                        
ALTER TABLE my_table SET TBLPROPERTIES (                                                                                                                                                                                                
'write.delete.encoding-hint' = 'auto'                                                                                                                                                                                                 
);
                                                                                                                                                                                                                                          
---                                                                                                                                                                                                                                     
5. Implementation Strategy

5.1 Phase 1: Core Infrastructure (Week 1-2)

Goals:
- Add encoding field to spec
- Implement DeleteEncoding enum
- Add validation logic
- Update manifest serialization/deserialization

Files to modify:                                                                                                                                                                                                                        
core/                                                                                                                                                                                                                                   
├── src/main/java/org/apache/iceberg/                                                                                                                                                                                                   
│   ├── DeleteFile.java (add encoding() method)                                                                                                                                                                                         
│   ├── DeleteEncoding.java (NEW)                                                                                                                                                                                                       
│   ├── GenericDeleteFile.java (implement encoding field)                                                                                                                                                                               
│   └── ManifestEntry.java (serialize encoding)                                                                                                                                                                                         
└── src/main/avro/                                                                                                                                                                                                                      
└── manifest-entry.avsc (add encoding field)

Implementation:

// GenericDeleteFile.java                                                                                                                                                                                                               
public class GenericDeleteFile implements DeleteFile {                                                                                                                                                                                  
private DeleteEncoding encoding;

    // Constructor                                                                                                                                                                                                                        
    public GenericDeleteFile(                                                                                                                                                                                                             
        FileContent content,                                                                                                                                                                                                              
        CharSequence path,                                                                                                                                                                                                                
        FileFormat format,                                                                                                                                                                                                                
        long recordCount,                                                                                                                                                                                                                 
        long fileSizeInBytes,                                                                                                                                                                                                             
        DeleteEncoding encoding) {  // NEW parameter                                                                                                                                                                                      
      // ...                                                                                                                                                                                                                              
      this.encoding = encoding != null ? encoding : DeleteEncoding.FULL_ROW;                                                                                                                                                              
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Override                                                                                                                                                                                                                             
    public DeleteEncoding encoding() {                                                                                                                                                                                                    
      return encoding;                                                                                                                                                                                                                    
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    // Builder                                                                                                                                                                                                                            
    public static class Builder {                                                                                                                                                                                                         
      private DeleteEncoding encoding = DeleteEncoding.FULL_ROW;                                                                                                                                                                          
                                                                                                                                                                                                                                          
      public Builder encoding(DeleteEncoding enc) {                                                                                                                                                                                       
        this.encoding = enc;                                                                                                                                                                                                              
        return this;                                                                                                                                                                                                                      
      }                                                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
}

5.2 Phase 2: Writer Framework (Week 2-3)

Goals:
- Implement DeleteWriterFactory
- Create automatic selection logic
- Implement cost estimator

Files to create:                                                                                                                                                                                                                        
core/src/main/java/org/apache/iceberg/io/                                                                                                                                                                                               
├── DeleteWriterFactory.java (NEW)                                                                                                                                                                                                      
├── EncodingCostEstimator.java (NEW)                                                                                                                                                                                                    
└── DeletionVectorEqualityWriter.java (refactor from BitmapDeleteWriter)

Key changes:

1. Rename BitmapDeleteWriter → DeletionVectorWriter                                                                                                                                                                                     
   // Old name was too specific                                                                                                                                                                                                            
   public class DeletionVectorWriter implements DeleteWriter {

    // Support both position and equality deletes                                                                                                                                                                                         
    public void writePositionDelete(long position) { ... }                                                                                                                                                                                
    public void writeEqualityDelete(long value) { ... }                                                                                                                                                                                   
}
2. Create factory pattern:                                                                                                                                                                                                              
   DeleteWriter writer = DeleteWriterFactory                                                                                                                                                                                               
   .forTable(table)                                                                                                                                                                                                                    
   .createEqualityWriter(equalityFieldIds, deleteSchema);

// Factory internally decides: DV or full-row

5.3 Phase 3: Reader Enhancement (Week 3)

Goals:
- Update BaseDeleteLoader to handle encoding detection
- Ensure transparent loading of all encodings
- Maintain backward compatibility

Files to modify:                                                                                                                                                                                                                        
data/src/main/java/org/apache/iceberg/data/                                                                                                                                                                                             
├── BaseDeleteLoader.java (enhance)                                                                                                                                                                                                     
└── DeleteFilter.java (no changes - already abstracted)

Key changes:

public class BaseDeleteLoader implements DeleteLoader {

    @Override                                                                                                                                                                                                                             
    public Set<StructLike> loadEqualityDeletes(                                                                                                                                                                                           
        Iterable<DeleteFile> deleteFiles,                                                                                                                                                                                                 
        Schema projection) {                                                                                                                                                                                                              
                                                                                                                                                                                                                                          
      // Group by encoding                                                                                                                                                                                                                
      Map<DeleteEncoding, List<DeleteFile>> byEncoding =                                                                                                                                                                                  
          StreamSupport.stream(deleteFiles.spliterator(), false)                                                                                                                                                                          
              .collect(Collectors.groupingBy(DeleteFile::encoding));                                                                                                                                                                      
                                                                                                                                                                                                                                          
      StructLikeSet result = StructLikeSet.create(projection.asStruct());                                                                                                                                                                 
                                                                                                                                                                                                                                          
      // Load deletion vectors                                                                                                                                                                                                            
      List<DeleteFile> dvFiles = byEncoding.get(DeleteEncoding.DELETION_VECTOR);                                                                                                                                                          
      if (dvFiles != null) {                                                                                                                                                                                                              
        for (DeleteFile file : dvFiles) {                                                                                                                                                                                                 
          result.addAll(loadDeletionVectorEquality(file, projection));                                                                                                                                                                    
        }                                                                                                                                                                                                                                 
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      // Load full-row deletes                                                                                                                                                                                                            
      List<DeleteFile> fullRowFiles = byEncoding.get(DeleteEncoding.FULL_ROW);                                                                                                                                                            
      if (fullRowFiles != null) {                                                                                                                                                                                                         
        result.addAll(loadFullRowEquality(fullRowFiles, projection));                                                                                                                                                                     
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      return result;                                                                                                                                                                                                                      
    }                                                                                                                                                                                                                                     
}

5.4 Phase 4: Spark Integration (Week 4)

Goals:
- Update Spark writers to use new factory
- Ensure automatic selection works in Spark SQL
- Fix table refresh issue

Files to modify:                                                                                                                                                                                                                        
spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/                                                                                                                                                                         
├── SparkPositionDeltaWrite.java                                                                                                                                                                                                        
├── RowLevelOperationContext.java                                                                                                                                                                                                       
└── SparkWrite.java

Key changes:

1. Remove manual strategy selection:                                                                                                                                                                                                    
   // OLD (current implementation)                                                                                                                                                                                                         
   if (shouldUseEqualityDeletes()) {                                                                                                                                                                                                       
   return new SparkEqualityDeltaWrite(...);                                                                                                                                                                                              
   }

// NEW (automatic)                                                                                                                                                                                                                      
DeleteWriter writer = DeleteWriterFactory                                                                                                                                                                                               
.forTable(table)                                                                                                                                                                                                                    
.createEqualityWriter(equalityFieldIds, schema);                                                                                                                                                                                    
// Factory decides encoding internally
2. Auto-refresh Spark catalog after commits:                                                                                                                                                                                            
   public class SparkCatalogUtil {                                                                                                                                                                                                         
   public static void commitAndRefresh(Table table, RowDelta delta) {                                                                                                                                                                    
   delta.commit();

   // Auto-refresh Spark's cached metadata                                                                                                                                                                                             
   if (table instanceof SparkTable) {                                                                                                                                                                                                  
   ((SparkTable) table).invalidateCaches();                                                                                                                                                                                          
   }                                                                                                                                                                                                                                   
   }                                                                                                                                                                                                                                     
   }

5.5 Phase 5: Testing & Documentation (Week 4)

Tests to add:
1. Encoding serialization/deserialization
2. Automatic format selection
3. Mixed encoding scenarios (DV + full-row)
4. Backward compatibility (reading old files without encoding field)
5. Type validation (only LONG for DV)

Documentation:
- Update spec documentation
- Add design doc to repo
- Update user guides
- Add javadocs for new APIs

  ---                                                                                                                                                                                                                                     
6. Migration and Compatibility

6.1 Backward Compatibility

Reading old delete files:                                                                                                                                                                                                               
public DeleteEncoding encoding() {                                                                                                                                                                                                      
// For files written before encoding field was added                                                                                                                                                                                  
if (this.encoding == null) {                                                                                                                                                                                                          
// Infer from format                                                                                                                                                                                                                
if (format == FileFormat.PUFFIN) {                                                                                                                                                                                                  
return DeleteEncoding.DELETION_VECTOR;                                                                                                                                                                                            
}                                                                                                                                                                                                                                   
return DeleteEncoding.FULL_ROW;                                                                                                                                                                                                     
}                                                                                                                                                                                                                                     
return this.encoding;                                                                                                                                                                                                                 
}

Writing with old readers:
- Old readers ignore unknown Avro fields → they see encoding as null
- Inference logic (above) handles it correctly
- ✅ Full backward compatibility

6.2 Forward Compatibility

New encodings added in future:                                                                                                                                                                                                          
// Reader version 1.5.0                                                                                                                                                                                                                 
DeleteEncoding encoding = file.encoding();                                                                                                                                                                                              
switch (encoding) {                                                                                                                                                                                                                     
case FULL_ROW:                                                                                                                                                                                                                        
case DELETION_VECTOR:                                                                                                                                                                                                                 
// Supported                                                                                                                                                                                                                        
break;                                                                                                                                                                                                                              
case HASH_MAP:  // Added in version 2.0                                                                                                                                                                                               
throw new UnsupportedOperationException(                                                                                                                                                                                            
"Encoding " + encoding + " requires Iceberg 2.0+");                                                                                                                                                                             
default:                                                                                                                                                                                                                              
// Unknown encoding - fail fast                                                                                                                                                                                                     
throw new IllegalStateException(                                                                                                                                                                                                    
"Unknown encoding: " + encoding);                                                                                                                                                                                               
}

6.3 Migration Path

Existing deployments:

1. v1.4.x → v1.5.0 (this feature)                                                                                                                                                                                                       
   - No breaking changes                                                                                                                                                                                                                 
   - Old delete files work as before                                                                                                                                                                                                     
   - New delete files use encoding field                                                                                                                                                                                                 
   - Readers gracefully handle both
2. Rewriting old delete files (optional)                                                                                                                                                                                                
   // Optional optimization: rewrite full-row deletes as DVs                                                                                                                                                                               
   RewriteDeleteFiles rewrite = table.rewriteDeleteFiles()                                                                                                                                                                                 
   .option("target-encoding", "deletion-vector")                                                                                                                                                                                       
   .filter(Expressions.equal("encoding", "full-row"));                                                                                                                                                                                 
   rewrite.execute();

  ---                                                                                                                                                                                                                                     
7. Extension Points

7.1 Type Support Extension

Current (v1): Only LONG type

Future extension design:

public interface DeletionVectorCodec<T> {                                                                                                                                                                                               
/**                                                                                                                                                                                                                                   
* Encodes a value into a long suitable for bitmap storage.                                                                                                                                                                           
*/                                                                                                                                                                                                                                   
long encode(T value);

    /**                                                                                                                                                                                                                                   
     * Decodes a long from bitmap back to the original value.                                                                                                                                                                             
     */                                                                                                                                                                                                                                   
    T decode(long encoded);                                                                                                                                                                                                               
                                                                                                                                                                                                                                          
    /**                                                                                                                                                                                                                                   
     * The Iceberg type this codec supports.                                                                                                                                                                                              
     */                                                                                                                                                                                                                                   
    Type.TypeID supportedType();                                                                                                                                                                                                          
}

// Registry                                                                                                                                                                                                                             
public class DeletionVectorCodecs {                                                                                                                                                                                                     
private static final Map<Type.TypeID, DeletionVectorCodec<?>> CODECS =                                                                                                                                                                
new HashMap<>();

    static {                                                                                                                                                                                                                              
      // v1: Only LONG (identity codec)                                                                                                                                                                                                   
      register(new LongCodec());                                                                                                                                                                                                          
                                                                                                                                                                                                                                          
      // v2: Add more types                                                                                                                                                                                                               
      // register(new IntCodec());                                                                                                                                                                                                        
      // register(new DateCodec());                                                                                                                                                                                                       
      // register(new TimestampCodec());                                                                                                                                                                                                  
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    public static <T> DeletionVectorCodec<T> getCodec(Type type) {                                                                                                                                                                        
      return (DeletionVectorCodec<T>) CODECS.get(type.typeId());                                                                                                                                                                          
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    public static void register(DeletionVectorCodec<?> codec) {                                                                                                                                                                           
      CODECS.put(codec.supportedType(), codec);                                                                                                                                                                                           
    }                                                                                                                                                                                                                                     
}

// Example: INT codec (future)                                                                                                                                                                                                          
class IntCodec implements DeletionVectorCodec<Integer> {                                                                                                                                                                                
@Override                                                                                                                                                                                                                             
public long encode(Integer value) {                                                                                                                                                                                                   
return value.longValue();  // Simple cast                                                                                                                                                                                           
}

    @Override                                                                                                                                                                                                                             
    public Integer decode(long encoded) {                                                                                                                                                                                                 
      return (int) encoded;                                                                                                                                                                                                               
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Override                                                                                                                                                                                                                             
    public Type.TypeID supportedType() {                                                                                                                                                                                                  
      return Type.TypeID.INTEGER;                                                                                                                                                                                                         
    }                                                                                                                                                                                                                                     
}

// Example: STRING codec (future, using dictionary)                                                                                                                                                                                     
class StringCodec implements DeletionVectorCodec<String> {                                                                                                                                                                              
private final BiMap<String, Long> dictionary = HashBiMap.create();                                                                                                                                                                    
private final AtomicLong nextId = new AtomicLong(0);

    @Override                                                                                                                                                                                                                             
    public long encode(String value) {                                                                                                                                                                                                    
      return dictionary.computeIfAbsent(value,                                                                                                                                                                                            
          k -> nextId.getAndIncrement());                                                                                                                                                                                                 
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Override                                                                                                                                                                                                                             
    public String decode(long encoded) {                                                                                                                                                                                                  
      return dictionary.inverse().get(encoded);                                                                                                                                                                                           
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Override                                                                                                                                                                                                                             
    public Type.TypeID supportedType() {                                                                                                                                                                                                  
      return Type.TypeID.STRING;                                                                                                                                                                                                          
    }                                                                                                                                                                                                                                     
}

Usage in DeletionVectorWriter:                                                                                                                                                                                                          
public class DeletionVectorWriter<T> implements EqualityDeleteWriter<T> {                                                                                                                                                               
private final DeletionVectorCodec<T> codec;                                                                                                                                                                                           
private final RoaringBitmap bitmap = new RoaringBitmap();

    public DeletionVectorWriter(int fieldId, Schema schema) {                                                                                                                                                                             
      Type fieldType = schema.findType(fieldId);                                                                                                                                                                                          
      this.codec = DeletionVectorCodecs.getCodec(fieldType);                                                                                                                                                                              
                                                                                                                                                                                                                                          
      if (codec == null) {                                                                                                                                                                                                                
        throw new UnsupportedOperationException(                                                                                                                                                                                          
            "Deletion vectors not supported for type: " + fieldType);                                                                                                                                                                     
      }                                                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Override                                                                                                                                                                                                                             
    public void write(T value) {                                                                                                                                                                                                          
      long encoded = codec.encode(value);                                                                                                                                                                                                 
      bitmap.add((int) encoded);  // Assuming 32-bit for now                                                                                                                                                                              
    }                                                                                                                                                                                                                                     
}

Adding a new type becomes:
1. Implement DeletionVectorCodec<T>
2. Register in DeletionVectorCodecs
3. Update validation logic
4. ✅ No changes to core framework!

7.2 Composite Key Extension

Future: HASH_MAP encoding for multi-field equality deletes

public class HashMapDeleteWriter implements EqualityDeleteWriter<StructLike> {                                                                                                                                                          
// Use hash of composite key                                                                                                                                                                                                          
private final RoaringBitmap bitmap = new RoaringBitmap();                                                                                                                                                                             
private final List<Integer> equalityFieldIds;                                                                                                                                                                                         
private final StructLikeWrapper wrapper;

    @Override                                                                                                                                                                                                                             
    public void write(StructLike row) {                                                                                                                                                                                                   
      int hash = wrapper.wrap(row).hashCode();                                                                                                                                                                                            
      bitmap.add(hash);                                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    // Store original field values separately for hash collision resolution                                                                                                                                                               
    private final Map<Integer, List<StructLike>> collisionMap = new HashMap<>();                                                                                                                                                          
}

Validation update:                                                                                                                                                                                                                      
if (encoding == DeleteEncoding.HASH_MAP) {                                                                                                                                                                                              
// Future: multi-field support                                                                                                                                                                                                        
Preconditions.checkArgument(                                                                                                                                                                                                          
equalityFieldIds.size() >= 1,                                                                                                                                                                                                     
"Hash map encoding requires at least one field");                                                                                                                                                                                 
}

7.3 Pluggable Compression

Future: Allow different bitmap implementations

public interface BitmapCompression {                                                                                                                                                                                                    
byte[] compress(RoaringBitmap bitmap);                                                                                                                                                                                                
RoaringBitmap decompress(byte[] data);

    String name();                                                                                                                                                                                                                        
}

class RoaringCompression implements BitmapCompression { ... }                                                                                                                                                                           
class ConciseCompression implements BitmapCompression { ... }

// Table property                                                                                                                                                                                                                       
public static final String DV_COMPRESSION_ALGORITHM =                                                                                                                                                                                   
"write.deletion-vector.compression";
                                                                                                                                                                                                                                          
---                                                                                                                                                                                                                                     
8. Testing Strategy

8.1 Unit Tests

class DeleteEncodingTest {                                                                                                                                                                                                              
@Test                                                                                                                                                                                                                                 
public void testEncodingSerialization() {                                                                                                                                                                                             
DeleteFile file = new GenericDeleteFile(...)                                                                                                                                                                                        
.encoding(DeleteEncoding.DELETION_VECTOR);

      // Serialize to Avro                                                                                                                                                                                                                
      GenericRecord record = writeToAvro(file);                                                                                                                                                                                           
                                                                                                                                                                                                                                          
      // Deserialize                                                                                                                                                                                                                      
      DeleteFile deserialized = readFromAvro(record);                                                                                                                                                                                     
                                                                                                                                                                                                                                          
      assertEquals(DeleteEncoding.DELETION_VECTOR, deserialized.encoding());                                                                                                                                                              
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Test                                                                                                                                                                                                                                 
    public void testBackwardCompatibility() {                                                                                                                                                                                             
      // Simulate old file without encoding field                                                                                                                                                                                         
      GenericRecord oldRecord = createOldFormatRecord();                                                                                                                                                                                  
                                                                                                                                                                                                                                          
      DeleteFile file = readFromAvro(oldRecord);                                                                                                                                                                                          
                                                                                                                                                                                                                                          
      // Should infer from format                                                                                                                                                                                                         
      assertEquals(FileFormat.PARQUET, file.format());                                                                                                                                                                                    
      assertEquals(DeleteEncoding.FULL_ROW, file.encoding());                                                                                                                                                                             
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Test                                                                                                                                                                                                                                 
    public void testDVValidation() {                                                                                                                                                                                                      
      // DV must be PUFFIN                                                                                                                                                                                                                
      assertThrows(IllegalArgumentException.class, () -> {                                                                                                                                                                                
        new GenericDeleteFile(...)                                                                                                                                                                                                        
            .format(FileFormat.PARQUET)                                                                                                                                                                                                   
            .encoding(DeleteEncoding.DELETION_VECTOR)                                                                                                                                                                                     
            .validate();                                                                                                                                                                                                                  
      });                                                                                                                                                                                                                                 
    }                                                                                                                                                                                                                                     
}

class EncodingCostEstimatorTest {                                                                                                                                                                                                       
@Test                                                                                                                                                                                                                                 
public void testSparseDeletesPreferDV() {                                                                                                                                                                                             
EncodingCostEstimator estimator = new EncodingCostEstimator();

      // 1000 deletes out of 1M rows (0.1% selectivity)                                                                                                                                                                                   
      DeleteEncoding chosen = estimator.chooseEncoding(                                                                                                                                                                                   
          FileContent.EQUALITY_DELETES,                                                                                                                                                                                                   
          1000,                                                                                                                                                                                                                           
          1_000_000,                                                                                                                                                                                                                      
          schema,                                                                                                                                                                                                                         
          Collections.singletonList(1));                                                                                                                                                                                                  
                                                                                                                                                                                                                                          
      assertEquals(DeleteEncoding.DELETION_VECTOR, chosen);                                                                                                                                                                               
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Test                                                                                                                                                                                                                                 
    public void testDenseDeletesPreferFullRow() {                                                                                                                                                                                         
      EncodingCostEstimator estimator = new EncodingCostEstimator();                                                                                                                                                                      
                                                                                                                                                                                                                                          
      // 600K deletes out of 1M rows (60% selectivity)                                                                                                                                                                                    
      DeleteEncoding chosen = estimator.chooseEncoding(                                                                                                                                                                                   
          FileContent.EQUALITY_DELETES,                                                                                                                                                                                                   
          600_000,                                                                                                                                                                                                                        
          1_000_000,                                                                                                                                                                                                                      
          schema,                                                                                                                                                                                                                         
          Collections.singletonList(1));                                                                                                                                                                                                  
                                                                                                                                                                                                                                          
      assertEquals(DeleteEncoding.FULL_ROW, chosen);                                                                                                                                                                                      
    }                                                                                                                                                                                                                                     
}

8.2 Integration Tests

class TestUnifiedDeletionVectorFramework {

    @Test                                                                                                                                                                                                                                 
    public void testAutomaticEncodingSelection() {                                                                                                                                                                                        
      Table table = catalog.createTable(                                                                                                                                                                                                  
          TableIdentifier.of("db", "table"),                                                                                                                                                                                              
          schema,                                                                                                                                                                                                                         
          PartitionSpec.unpartitioned());                                                                                                                                                                                                 
                                                                                                                                                                                                                                          
      // Write data                                                                                                                                                                                                                       
      writeRecords(table, 1_000_000);                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
      // Delete using equality delete (sparse: 100 rows)                                                                                                                                                                                  
      DeleteWriter writer = DeleteWriterFactory                                                                                                                                                                                           
          .forTable(table)                                                                                                                                                                                                                
          .createEqualityWriter(                                                                                                                                                                                                          
              Collections.singletonList(1),                                                                                                                                                                                               
              schema);                                                                                                                                                                                                                    
                                                                                                                                                                                                                                          
      for (int i = 0; i < 100; i++) {                                                                                                                                                                                                     
        writer.write(GenericRecord.create(schema).set(0, (long) i));                                                                                                                                                                      
      }                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                          
      DeleteFile deleteFile = writer.close();                                                                                                                                                                                             
                                                                                                                                                                                                                                          
      // Verify automatic selection chose DV                                                                                                                                                                                              
      assertEquals(DeleteEncoding.DELETION_VECTOR, deleteFile.encoding());                                                                                                                                                                
      assertEquals(FileFormat.PUFFIN, deleteFile.format());                                                                                                                                                                               
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Test                                                                                                                                                                                                                                 
    public void testMixedEncodingRead() {                                                                                                                                                                                                 
      Table table = createTable();                                                                                                                                                                                                        
                                                                                                                                                                                                                                          
      // Create DV delete file                                                                                                                                                                                                            
      DeleteFile dv = createDeletionVectorFile(table, Arrays.asList(1L, 2L, 3L));                                                                                                                                                         
                                                                                                                                                                                                                                          
      // Create full-row delete file                                                                                                                                                                                                      
      DeleteFile fullRow = createFullRowFile(table, Arrays.asList(4L, 5L, 6L));                                                                                                                                                           
                                                                                                                                                                                                                                          
      // Commit both                                                                                                                                                                                                                      
      table.newRowDelta()                                                                                                                                                                                                                 
          .addDeletes(dv)                                                                                                                                                                                                                 
          .addDeletes(fullRow)                                                                                                                                                                                                            
          .commit();                                                                                                                                                                                                                      
                                                                                                                                                                                                                                          
      // Read and verify both are applied                                                                                                                                                                                                 
      DeleteLoader loader = new BaseDeleteLoader(...);                                                                                                                                                                                    
      Set<StructLike> deletes = loader.loadEqualityDeletes(                                                                                                                                                                               
          table.currentSnapshot().addedDeleteFiles(),                                                                                                                                                                                     
          schema);                                                                                                                                                                                                                        
                                                                                                                                                                                                                                          
      assertEquals(6, deletes.size());                                                                                                                                                                                                    
      // Verify all 6 values are in the set                                                                                                                                                                                               
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Test                                                                                                                                                                                                                                 
    public void testTableRefreshAutomatic() {                                                                                                                                                                                             
      SparkSession spark = ...;                                                                                                                                                                                                           
      Table table = catalog.loadTable(...);                                                                                                                                                                                               
                                                                                                                                                                                                                                          
      // Programmatic commit                                                                                                                                                                                                              
      DeleteFile file = createDeletionVectorFile(table, ...);                                                                                                                                                                             
      table.newRowDelta().addDeletes(file).commit();                                                                                                                                                                                      
                                                                                                                                                                                                                                          
      // Query immediately - should NOT require manual REFRESH                                                                                                                                                                            
      Dataset<Row> result = spark.sql("SELECT * FROM table");                                                                                                                                                                             
                                                                                                                                                                                                                                          
      // Verify deletes are applied                                                                                                                                                                                                       
      assertEquals(expectedCount, result.count());                                                                                                                                                                                        
    }                                                                                                                                                                                                                                     
}

8.3 Performance Benchmarks

@State(Scope.Benchmark)                                                                                                                                                                                                                 
public class DeletionVectorBenchmark {

    @Param({"100", "1000", "10000", "100000"})                                                                                                                                                                                            
    int deleteCount;                                                                                                                                                                                                                      
                                                                                                                                                                                                                                          
    @Param({"1000000"})                                                                                                                                                                                                                   
    int totalRows;                                                                                                                                                                                                                        
                                                                                                                                                                                                                                          
    @Benchmark                                                                                                                                                                                                                            
    public long benchmarkDVWrite() {                                                                                                                                                                                                      
      DeletionVectorWriter writer = new DeletionVectorWriter(...);                                                                                                                                                                        
      for (int i = 0; i < deleteCount; i++) {                                                                                                                                                                                             
        writer.write((long) i);                                                                                                                                                                                                           
      }                                                                                                                                                                                                                                   
      return writer.close().fileSizeInBytes();                                                                                                                                                                                            
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Benchmark                                                                                                                                                                                                                            
    public long benchmarkFullRowWrite() {                                                                                                                                                                                                 
      FullRowEqualityWriter writer = new FullRowEqualityWriter(...);                                                                                                                                                                      
      for (int i = 0; i < deleteCount; i++) {                                                                                                                                                                                             
        Record record = GenericRecord.create(schema);                                                                                                                                                                                     
        record.set(0, (long) i);                                                                                                                                                                                                          
        writer.write(record);                                                                                                                                                                                                             
      }                                                                                                                                                                                                                                   
      return writer.close().fileSizeInBytes();                                                                                                                                                                                            
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Benchmark                                                                                                                                                                                                                            
    public boolean benchmarkDVRead() {                                                                                                                                                                                                    
      Set<StructLike> deletes = loader.loadEqualityDeletes(dvFiles, schema);                                                                                                                                                              
      return deletes.contains(testRow);                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                          
    @Benchmark                                                                                                                                                                                                                            
    public boolean benchmarkFullRowRead() {                                                                                                                                                                                               
      Set<StructLike> deletes = loader.loadEqualityDeletes(fullRowFiles, schema);                                                                                                                                                         
      return deletes.contains(testRow);                                                                                                                                                                                                   
    }                                                                                                                                                                                                                                     
}
                                                                                                                                                                                                                                          
---                                                                                                                                                                                                                                     
9. Benefits of Unified Design

9.1 Simplicity

Before (current implementation):
- Table property: write.delete.strategy=equality
- Special writers: <USER_NAME>
- Special readers: isEqualityDeleteVector()
- User confusion: "When do I use what?"

After (unified design):
- No configuration needed
- Standard writers: DeleteWriterFactory.createEqualityWriter()
- Transparent readers: same DeleteLoader interface
- User experience: "It just works"

9.2 Extensibility

Adding a new type (e.g., INT):

Before:
- Modify BitmapDeleteWriter
- Modify BitmapBackedStructLikeSet
- Update validation logic in 5+ places
- High risk of breaking existing code

After:                                                                                                                                                                                                                                  
// 1. Implement codec                                                                                                                                                                                                                   
class IntCodec implements DeletionVectorCodec<Integer> { ... }

// 2. Register                                                                                                                                                                                                                          
DeletionVectorCodecs.register(new IntCodec());

// Done! Everything else works automatically

9.3 Consistency

All delete types treated uniformly:
- Position deletes: DV when file-level, full-row when partition-level
- Equality deletes: DV when single primitive, full-row when complex
- Same decision logic
- Same storage format (PUFFIN for DV)
- Same reading path

9.4 Forward Compatibility

Future encodings (e.g., HASH_MAP):                                                                                                                                                                                                      
// Add to enum                                                                                                                                                                                                                          
HASH_MAP(2)

// Implement writer                                                                                                                                                                                                                     
class HashMapDeleteWriter { ... }

// Update factory                                                                                                                                                                                                                       
if (equalityFieldIds.size() > 1 && supportsHashMap()) {                                                                                                                                                                                 
return new HashMapDeleteWriter(...);                                                                                                                                                                                                  
}

// Old readers fail gracefully                                                                                                                                                                                                          
throw new UnsupportedOperationException(                                                                                                                                                                                                
"HASH_MAP encoding requires Iceberg 2.0+");
                                                                                                                                                                                                                                          
---                                                                                                                                                                                                                                     
10. Migration from Current Implementation

10.1 What to Keep

✅ Keep (good abstractions):
1. BitmapDeleteWriter core logic → rename to DeletionVectorWriter
2. BitmapBackedStructLikeSet → keep as-is
3. BaseDeleteLoader.readEqualityDeleteVector() → keep core logic
4. PUFFIN format usage → perfect choice
5. Test infrastructure → excellent coverage

10.2 What to Change

❌ Remove/refactor:
1. write.delete.strategy table property → replace with write.delete.encoding-hint
2. Manual format selection in SparkPositionDeltaWriteBuilder → use factory
3. shouldUseEqualityDeletes() logic → move to factory
4. Implicit encoding detection → make explicit with encoding field

10.3 Refactoring Steps

Step 1: Add encoding field to spec (backward compatible)                                                                                                                                                                                
// Can be done independently                                                                                                                                                                                                            
ALTER TABLE ... // old tables still work

Step 2: Create DeleteWriterFactory (parallel implementation)                                                                                                                                                                            
// New code path, doesn't affect existing                                                                                                                                                                                               
DeleteWriter writer = DeleteWriterFactory.create(...);

Step 3: Update Spark to use factory (feature flag)                                                                                                                                                                                      
if (useUnifiedDV) {                                                                                                                                                                                                                     
return factory.createWriter(...);                                                                                                                                                                                                     
} else {                                                                                                                                                                                                                                
return legacyWriter(...);  // old path                                                                                                                                                                                                
}

Step 4: Migrate readers to handle encoding field                                                                                                                                                                                        
// Graceful fallback to old inference logic                                                                                                                                                                                             
if (file.encoding() != null) { ... }                                                                                                                                                                                                    
else { /* infer */ }

Step 5: Deprecate old APIs                                                                                                                                                                                                              
@Deprecated                                                                                                                                                                                                                             
public static final String DELETE_STRATEGY = ...;

Step 6: Remove after 2 releases
                                                                                                                                                                                                                                          
---                                                                                                                                                                                                                                     
Summary

This unified Deletion Vector framework provides:

1. Single abstraction for all delete types
2. Automatic encoding selection - no user configuration
3. Explicit metadata - encoding field in spec
4. Type extensibility - codec pattern for new types
5. Forward compatibility - graceful handling of future encodings
6. Simplified UX - "it just works"

Key Difference from Current Implementation:
- Current: "Equality Delete Vectors" as a separate feature
- Unified: "Deletion Vectors" as a storage optimization for all delete types

Recommendation: Refactor current implementation to use this unified design before merging to main branch.

✻ Baked for 3m 20s

● How is Claude doing this session? (optional)                                                                                                                                                                                            
1: Bad    2: Fine   3: Good   0: Dismiss

──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
❯                                                                                                                                                                                                                                         
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
⏵⏵ bypass permissions on (shift+Tab to cycle)                                                                                                                                                                                           

