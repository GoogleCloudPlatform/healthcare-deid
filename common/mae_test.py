"""Tests for google3.dlp.mae."""

from __future__ import absolute_import

import unittest

from common import mae


class MaeTest(unittest.TestCase):

  def testGenerateDtd(self):
    mae_tag_categories = [
        {'name': 'type1'},
        {'name': 'type2'}
    ]
    dtd = mae.generate_dtd(mae_tag_categories, 'task_name')

    expected_dtd = """<!ENTITY name "task_name">

<!ELEMENT type1 ( #PCDATA ) >
<!ATTLIST type1 id ID prefix="type1" #REQUIRED >


<!ELEMENT type2 ( #PCDATA ) >
<!ATTLIST type2 id ID prefix="type2" #REQUIRED >
"""
    self.assertEqual(dtd, expected_dtd)

  def testGenerateMae(self):
    inspect_result = {
        'patient_id': '111',
        'record_number': '2',
        'original_note': 'this is the note with the PHI',
        'result': {
            'findings': [
                {
                    'infoType': {'name': 'infoTypeA'},
                    'location': {'codepointRange': {'start': '1', 'end': '5'}}
                }
            ]
        }
    }
    mae_tag_categories = [
        {'name': 'TagA', 'infoTypes': ['infoTypeA']},
        {'name': 'TagB', 'infoTypes': ['infoTypeUnused']}
    ]
    key_columns = ['patient_id', 'record_number']
    result = mae.generate_mae(inspect_result, 'task_name', mae_tag_categories,
                              key_columns)
    expected = """<?xml version="1.0" encoding="UTF-8" ?>
<task_name>
<TEXT><![CDATA[this is the note with the PHI]]></TEXT>
<TAGS>
<TagA id="TagA0" spans="1~5" />
</TAGS></task_name>
"""
    self.assertEqual(expected, result.mae_xml)
    self.assertEqual('111-2', result.record_id)

  def testGenerateMaeInvalidControlChar(self):
    # The original note has characters which are not valid XML, so we remove
    # them when generating the XML, and adjust the findings to fit the new
    # string.
    inspect_result = {
        'patient_id': '111',
        'record_number': '2',
        'original_note': 'invalid chars: \x0c and \x00\x01\x02\x03.',
        'result': {
            'findings': [
                {'infoType': {'name': 'infoTypeA'},
                 'location': {'codepointRange': {'start': '1', 'end': '5'}}},
                {'infoType': {'name': 'infoTypeA'},
                 'location': {'codepointRange': {'start': '14', 'end': '16'}}},
                {'infoType': {'name': 'infoTypeA'},
                 'location': {'codepointRange': {'start': '16', 'end': '18'}}},
                {'infoType': {'name': 'infoTypeA'},
                 'location': {'codepointRange': {'start': '18', 'end': '25'}}}
            ]
        }
    }
    mae_tag_categories = [
        {'name': 'TagA', 'infoTypes': ['infoTypeA']},
        {'name': 'TagB', 'infoTypes': ['infoTypeUnused']}
    ]
    key_columns = ['patient_id', 'record_number']
    result = mae.generate_mae(inspect_result, 'task_name', mae_tag_categories,
                              key_columns)
    expected = """<?xml version="1.0" encoding="UTF-8" ?>
<task_name>
<TEXT><![CDATA[invalid chars:  and .]]></TEXT>
<TAGS>
<TagA id="TagA0" spans="1~5" />
<TagA id="TagA1" spans="14~15" />
<TagA id="TagA2" spans="15~17" />
<TagA id="TagA3" spans="17~20" />
</TAGS></task_name>
"""
    self.assertEqual(expected, result.mae_xml)
    self.assertEqual('111-2', result.record_id)

if __name__ == '__main__':
  unittest.main()
