import unittest
from main import parse_slaves


class TestParseSlaves(unittest.TestCase):
    def test_only_id(self):
        slaves = parse_slaves(['1', '2'])
        self.assertEqual(len(slaves), 2)
        slave = slaves[0]
        self.assertEqual(slave['id'], 1)
        self.assertEqual(slave['name'], '')
        slave = slaves[1]
        self.assertEqual(slave['id'], 2)
        self.assertEqual(slave['name'], '')

    def test_id_name_1(self):
        slaves = parse_slaves(['1', 'Name 1', '3'])
        self.assertEqual(len(slaves), 2)
        slave = slaves[0]
        self.assertEqual(slave['id'], 1)
        self.assertEqual(slave['name'], 'Name 1')
        slave = slaves[1]
        self.assertEqual(slave['id'], 3)
        self.assertEqual(slave['name'], '')

    def test_id_name_2(self):
        slaves = parse_slaves(['1', '3', 'Name 3', '4'])
        self.assertEqual(len(slaves), 3)
        slave = slaves[0]
        self.assertEqual(slave['id'], 1)
        self.assertEqual(slave['name'], '')
        slave = slaves[1]
        self.assertEqual(slave['id'], 3)
        self.assertEqual(slave['name'], 'Name 3')
        slave = slaves[2]
        self.assertEqual(slave['id'], 4)
        self.assertEqual(slave['name'], '')

    def test_id_name_all(self):
        slaves = parse_slaves(['1', 'Name 1', '3', 'Name 3', '4', 'Name 4'])
        self.assertEqual(len(slaves), 3)
        slave = slaves[0]
        self.assertEqual(slave['id'], 1)
        self.assertEqual(slave['name'], 'Name 1')
        slave = slaves[1]
        self.assertEqual(slave['id'], 3)
        self.assertEqual(slave['name'], 'Name 3')
        slave = slaves[2]
        self.assertEqual(slave['id'], 4)
        self.assertEqual(slave['name'], 'Name 4')

    def test_name_no_id(self):
        slaves = parse_slaves(['1', 'Name 1', 'Name 2', '3'])
        self.assertEqual(len(slaves), 2)
        slave = slaves[0]
        self.assertEqual(slave['id'], 1)
        self.assertEqual(slave['name'], 'Name 1')
        slave = slaves[1]
        self.assertEqual(slave['id'], 3)
        self.assertEqual(slave['name'], '')


if __name__ == '__main__':
    unittest.main()
