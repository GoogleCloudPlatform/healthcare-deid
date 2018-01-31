"""Tests for google3.third_party.py.dlp.mae."""

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
    result = mae.generate_mae(inspect_result, 'task_name', mae_tag_categories)
    expected = """<?xml version="1.0" encoding="UTF-8" ?>
<task_name>
<TEXT><![CDATA[this is the note with the PHI]]></TEXT>
<TAGS>
<TagA id="TagA0" spans="1~5" />
</TAGS></task_name>
"""
    self.assertEqual(expected, result.mae_xml)

if __name__ == '__main__':
  unittest.main()
