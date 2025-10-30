// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.healthcare.imaging.dicomadapter.cstore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.dcm4che3.data.Attributes;

/**
 * Filter for skipping DICOM instances based on tag values.
 *
 * <p>This filter allows selective skipping of DICOM instances during C-STORE operations
 * based on specific tag-value conditions. When a match is found, the instance is not stored
 * but a success response is still returned to the client.
 *
 * <p>Example use case: Filtering out RAW mammography images (SOP Class UID
 * 1.2.840.10008.5.1.4.1.1.1.2.1) that are not intended for viewing.
 */
public class SkipTagFilter {

  /**
   * Represents a single skip condition: a DICOM tag and its expected value.
   */
  public static class SkipCondition {
    private final int tag;
    private final String expectedValue;

    /**
     * Creates a new skip condition.
     *
     * @param tag DICOM tag as integer (e.g., 0x00080016 for SOP Class UID)
     * @param expectedValue Expected string value to match
     */
    public SkipCondition(int tag, String expectedValue) {
      this.tag = tag;
      this.expectedValue = expectedValue;
    }

    /**
     * @return The DICOM tag to check
     */
    public int getTag() {
      return tag;
    }

    /**
     * @return The expected value for this tag
     */
    public String getExpectedValue() {
      return expectedValue;
    }
  }

  private final List<SkipCondition> conditions;

  /**
   * Creates a new SkipTagFilter with the given conditions.
   *
   * @param conditions List of skip conditions (OR logic - any match triggers skip)
   */
  public SkipTagFilter(List<SkipCondition> conditions) {
    // Defensive copy to prevent external modification
    this.conditions = new ArrayList<>(conditions);
  }

  /**
   * Checks if a DICOM instance should be skipped based on the configured conditions.
   *
   * <p>Uses OR logic: if ANY condition matches, the instance is skipped.
   *
   * @param attrs DICOM attributes to check
   * @return true if the instance should be skipped, false otherwise
   */
  public boolean shouldSkip(Attributes attrs) {
    for (SkipCondition condition : conditions) {
      String actualValue = attrs.getString(condition.getTag());
      if (actualValue != null && actualValue.equals(condition.getExpectedValue())) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return Unmodifiable list of configured skip conditions
   */
  public List<SkipCondition> getConditions() {
    return Collections.unmodifiableList(conditions);
  }
}
