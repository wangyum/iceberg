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

public class TestContentFileUtilHelpers {

  @Test
  public void testIsPositionDV() {
    DeleteFile positionDV = mock(DeleteFile.class);
    when(positionDV.format()).thenReturn(FileFormat.PUFFIN);
    when(positionDV.content()).thenReturn(FileContent.POSITION_DELETES);
    assertThat(ContentFileUtil.isPositionDV(positionDV)).isTrue();

    DeleteFile parquetDelete = mock(DeleteFile.class);
    when(parquetDelete.format()).thenReturn(FileFormat.PARQUET);
    assertThat(ContentFileUtil.isPositionDV(parquetDelete)).isFalse();
  }

  @Test
  public void testIsEqualityDV() {
    DeleteFile equalityDV = mock(DeleteFile.class);
    when(equalityDV.format()).thenReturn(FileFormat.PUFFIN);
    when(equalityDV.content()).thenReturn(FileContent.EQUALITY_DELETES);
    assertThat(ContentFileUtil.isEqualityDV(equalityDV)).isTrue();

    DeleteFile parquetDelete = mock(DeleteFile.class);
    when(parquetDelete.format()).thenReturn(FileFormat.PARQUET);
    assertThat(ContentFileUtil.isEqualityDV(parquetDelete)).isFalse();
  }
}
