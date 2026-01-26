/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.Test;

/** Tests for ContentFileUtil helper methods. */
public class TestContentFileUtilHelpers {

  @Test
  public void testDvTypeDescription() {
    // Position DV (PUFFIN format, POSITION_DELETES content)
    DeleteFile positionDV = mock(DeleteFile.class);
    when(positionDV.format()).thenReturn(FileFormat.PUFFIN);
    when(positionDV.content()).thenReturn(FileContent.POSITION_DELETES);
    assertThat(ContentFileUtil.dvTypeDescription(positionDV)).isEqualTo("Position DV");

    // Equality DV (PUFFIN format, EQUALITY_DELETES content)
    DeleteFile equalityDV = mock(DeleteFile.class);
    when(equalityDV.format()).thenReturn(FileFormat.PUFFIN);
    when(equalityDV.content()).thenReturn(FileContent.EQUALITY_DELETES);
    assertThat(ContentFileUtil.dvTypeDescription(equalityDV)).isEqualTo("Equality DV");

    // Traditional position delete (Parquet format, POSITION_DELETES content)
    DeleteFile traditionalPosition = mock(DeleteFile.class);
    when(traditionalPosition.format()).thenReturn(FileFormat.PARQUET);
    when(traditionalPosition.content()).thenReturn(FileContent.POSITION_DELETES);
    assertThat(ContentFileUtil.dvTypeDescription(traditionalPosition)).isEqualTo("Not a DV");

    // Traditional equality delete (Parquet format, EQUALITY_DELETES content)
    DeleteFile traditionalEquality = mock(DeleteFile.class);
    when(traditionalEquality.format()).thenReturn(FileFormat.PARQUET);
    when(traditionalEquality.content()).thenReturn(FileContent.EQUALITY_DELETES);
    assertThat(ContentFileUtil.dvTypeDescription(traditionalEquality)).isEqualTo("Not a DV");
  }

  @Test
  public void testIsDVType() {
    // Position DV
    DeleteFile positionDV = mock(DeleteFile.class);
    when(positionDV.format()).thenReturn(FileFormat.PUFFIN);
    when(positionDV.content()).thenReturn(FileContent.POSITION_DELETES);
    assertThat(ContentFileUtil.isDVType(positionDV, FileContent.POSITION_DELETES)).isTrue();
    assertThat(ContentFileUtil.isDVType(positionDV, FileContent.EQUALITY_DELETES)).isFalse();

    // Equality DV
    DeleteFile equalityDV = mock(DeleteFile.class);
    when(equalityDV.format()).thenReturn(FileFormat.PUFFIN);
    when(equalityDV.content()).thenReturn(FileContent.EQUALITY_DELETES);
    assertThat(ContentFileUtil.isDVType(equalityDV, FileContent.EQUALITY_DELETES)).isTrue();
    assertThat(ContentFileUtil.isDVType(equalityDV, FileContent.POSITION_DELETES)).isFalse();

    // Traditional position delete (Parquet) - not a DV
    DeleteFile traditionalPosition = mock(DeleteFile.class);
    when(traditionalPosition.format()).thenReturn(FileFormat.PARQUET);
    when(traditionalPosition.content()).thenReturn(FileContent.POSITION_DELETES);
    assertThat(ContentFileUtil.isDVType(traditionalPosition, FileContent.POSITION_DELETES))
        .isFalse();

    // Traditional equality delete (Parquet) - not a DV
    DeleteFile traditionalEquality = mock(DeleteFile.class);
    when(traditionalEquality.format()).thenReturn(FileFormat.PARQUET);
    when(traditionalEquality.content()).thenReturn(FileContent.EQUALITY_DELETES);
    assertThat(ContentFileUtil.isDVType(traditionalEquality, FileContent.EQUALITY_DELETES))
        .isFalse();
  }

  @Test
  public void testIsDVTypeConsistencyWithExistingMethods() {
    // Position DV
    DeleteFile positionDV = mock(DeleteFile.class);
    when(positionDV.format()).thenReturn(FileFormat.PUFFIN);
    when(positionDV.content()).thenReturn(FileContent.POSITION_DELETES);
    assertThat(ContentFileUtil.isDVType(positionDV, FileContent.POSITION_DELETES))
        .isEqualTo(ContentFileUtil.isPositionDV(positionDV));

    // Equality DV
    DeleteFile equalityDV = mock(DeleteFile.class);
    when(equalityDV.format()).thenReturn(FileFormat.PUFFIN);
    when(equalityDV.content()).thenReturn(FileContent.EQUALITY_DELETES);
    assertThat(ContentFileUtil.isDVType(equalityDV, FileContent.EQUALITY_DELETES))
        .isEqualTo(ContentFileUtil.isEqualityDV(equalityDV));
  }
}

