import unittest
import pickle
import chatanalytics # to be run in base directory


class DiscordChatTest(unittest.TestCase):
    baseline_path = "test/test_data/discord/baseline/"
    raw_data_path = "test/test_data/discord/messages/"
    direct_message_path = "c5338959842695873"
    group_message_path = "c5662031163313723"
    server_message_path = "c5519593417670182"

    def test_direct_message_import(self):
        group = chatanalytics.DiscordChat()
        group.load(self.raw_data_path + self.direct_message_path)
        with open(self.baseline_path + self.direct_message_path + ".p", "rb") as f:
            baseline = pickle.load(f)
            self.assertEqual(group, baseline)

    def test_group_message_import(self):
        group = chatanalytics.DiscordChat()
        group.load(self.raw_data_path + self.group_message_path)
        with open(self.baseline_path + self.group_message_path + ".p", "rb") as f:
            baseline = pickle.load(f)
            self.assertEqual(group, baseline)

    def test_server_message_import(self):
        group = chatanalytics.DiscordChat()
        group.load(self.raw_data_path + self.server_message_path)
        with open(self.baseline_path + self.server_message_path + ".p", "rb") as f:
            baseline = pickle.load(f)
            self.assertEqual(group, baseline)

    def test_batch_import(self):
        group = chatanalytics.DiscordChat()
        group.batch_load(self.raw_data_path)
        with open(self.baseline_path + "batch.p", "rb") as f:
            baseline = pickle.load(f)
            self.assertEqual(group, baseline)
