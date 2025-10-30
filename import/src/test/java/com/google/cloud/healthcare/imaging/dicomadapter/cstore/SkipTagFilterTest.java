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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.junit.Test;

/**
 * Unit tests for SkipTagFilter.
 */
public class SkipTagFilterTest {

  private static final String RAW_MAMMOGRAPHY_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.1.2.1";
  private static final String PROCESSED_MAMMOGRAPHY_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.1.2";
  private static final String FOR_PROCESSING = "FOR PROCESSING";
  private static final String FOR_PRESENTATION = "FOR PRESENTATION";

  @Test
  public void testShouldSkip_SingleCondition_Matches() {
    // Create filter with SOP Class UID condition
    List<SkipTagFilter.SkipCondition> conditions = new ArrayList<>();
    conditions.add(new SkipTagFilter.SkipCondition(Tag.SOPClassUID, RAW_MAMMOGRAPHY_SOP_CLASS));
    SkipTagFilter filter = new SkipTagFilter(conditions);

    // Create attributes with matching SOP Class UID
    Attributes attrs = new Attributes();
    attrs.setString(Tag.SOPClassUID, org.dcm4che3.data.VR.UI, RAW_MAMMOGRAPHY_SOP_CLASS);

    // Should skip
    assertTrue(filter.shouldSkip(attrs));
  }

  @Test
  public void testShouldSkip_SingleCondition_NoMatch() {
    // Create filter for RAW mammography
    List<SkipTagFilter.SkipCondition> conditions = new ArrayList<>();
    conditions.add(new SkipTagFilter.SkipCondition(Tag.SOPClassUID, RAW_MAMMOGRAPHY_SOP_CLASS));
    SkipTagFilter filter = new SkipTagFilter(conditions);

    // Create attributes with different SOP Class UID (processed)
    Attributes attrs = new Attributes();
    attrs.setString(Tag.SOPClassUID, org.dcm4che3.data.VR.UI, PROCESSED_MAMMOGRAPHY_SOP_CLASS);

    // Should NOT skip
    assertFalse(filter.shouldSkip(attrs));
  }

  @Test
  public void testShouldSkip_MultipleConditions_OneMatches() {
    // Create filter with 2 conditions
    List<SkipTagFilter.SkipCondition> conditions = new ArrayList<>();
    conditions.add(new SkipTagFilter.SkipCondition(Tag.SOPClassUID, RAW_MAMMOGRAPHY_SOP_CLASS));
    conditions.add(new SkipTagFilter.SkipCondition(Tag.PresentationIntentType, FOR_PROCESSING));
    SkipTagFilter filter = new SkipTagFilter(conditions);

    // Create attributes matching only second condition
    Attributes attrs = new Attributes();
    attrs.setString(Tag.SOPClassUID, org.dcm4che3.data.VR.UI, PROCESSED_MAMMOGRAPHY_SOP_CLASS);
    attrs.setString(Tag.PresentationIntentType, org.dcm4che3.data.VR.CS, FOR_PROCESSING);

    // Should skip (OR logic - any match triggers skip)
    assertTrue(filter.shouldSkip(attrs));
  }

  @Test
  public void testShouldSkip_MultipleConditions_BothMatch() {
    // Create filter with 2 conditions
    List<SkipTagFilter.SkipCondition> conditions = new ArrayList<>();
    conditions.add(new SkipTagFilter.SkipCondition(Tag.SOPClassUID, RAW_MAMMOGRAPHY_SOP_CLASS));
    conditions.add(new SkipTagFilter.SkipCondition(Tag.PresentationIntentType, FOR_PROCESSING));
    SkipTagFilter filter = new SkipTagFilter(conditions);

    // Create attributes matching both conditions
    Attributes attrs = new Attributes();
    attrs.setString(Tag.SOPClassUID, org.dcm4che3.data.VR.UI, RAW_MAMMOGRAPHY_SOP_CLASS);
    attrs.setString(Tag.PresentationIntentType, org.dcm4che3.data.VR.CS, FOR_PROCESSING);

    // Should skip
    assertTrue(filter.shouldSkip(attrs));
  }

  @Test
  public void testShouldSkip_MultipleConditions_NoneMatch() {
    // Create filter with 2 conditions
    List<SkipTagFilter.SkipCondition> conditions = new ArrayList<>();
    conditions.add(new SkipTagFilter.SkipCondition(Tag.SOPClassUID, RAW_MAMMOGRAPHY_SOP_CLASS));
    conditions.add(new SkipTagFilter.SkipCondition(Tag.PresentationIntentType, FOR_PROCESSING));
    SkipTagFilter filter = new SkipTagFilter(conditions);

    // Create attributes matching neither condition
    Attributes attrs = new Attributes();
    attrs.setString(Tag.SOPClassUID, org.dcm4che3.data.VR.UI, PROCESSED_MAMMOGRAPHY_SOP_CLASS);
    attrs.setString(Tag.PresentationIntentType, org.dcm4che3.data.VR.CS, FOR_PRESENTATION);

    // Should NOT skip
    assertFalse(filter.shouldSkip(attrs));
  }

  @Test
  public void testShouldSkip_TagMissing() {
    // Create filter for SOP Class UID
    List<SkipTagFilter.SkipCondition> conditions = new ArrayList<>();
    conditions.add(new SkipTagFilter.SkipCondition(Tag.SOPClassUID, RAW_MAMMOGRAPHY_SOP_CLASS));
    SkipTagFilter filter = new SkipTagFilter(conditions);

    // Create attributes without SOP Class UID
    Attributes attrs = new Attributes();
    attrs.setString(Tag.PatientName, org.dcm4che3.data.VR.PN, "Test Patient");

    // Should NOT skip (tag missing)
    assertFalse(filter.shouldSkip(attrs));
  }

  @Test
  public void testShouldSkip_EmptyConditions() {
    // Create filter with empty conditions list
    List<SkipTagFilter.SkipCondition> conditions = new ArrayList<>();
    SkipTagFilter filter = new SkipTagFilter(conditions);

    // Create attributes with some data
    Attributes attrs = new Attributes();
    attrs.setString(Tag.SOPClassUID, org.dcm4che3.data.VR.UI, RAW_MAMMOGRAPHY_SOP_CLASS);

    // Should NOT skip (no conditions to match)
    assertFalse(filter.shouldSkip(attrs));
  }

  @Test
  public void testShouldSkip_NullValue() {
    // Create filter for specific value
    List<SkipTagFilter.SkipCondition> conditions = new ArrayList<>();
    conditions.add(new SkipTagFilter.SkipCondition(Tag.SOPClassUID, RAW_MAMMOGRAPHY_SOP_CLASS));
    SkipTagFilter filter = new SkipTagFilter(conditions);

    // Create attributes with tag but null value
    Attributes attrs = new Attributes();
    // Don't set any value for SOPClassUID

    // Should NOT skip (null value doesn't match)
    assertFalse(filter.shouldSkip(attrs));
  }
}
