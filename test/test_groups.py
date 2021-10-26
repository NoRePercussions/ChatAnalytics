import unittest
import pickle
import chatanalytics  # to be run in base directory


class DiscordChatGroupTest(unittest.TestCase):
    baseline_path = "test/test_data/discord/baseline/"
    raw_data_path = "test/test_data/discord/messages/"
    direct_message_path = "c5338959842695873"
    group_message_path = "c5662031163313723"
    server_message_path = "c5519593417670182"

    def test_batch_import_works(self):
        group = chatanalytics.ChatGroup()
        group.batch_load(chatanalytics.DiscordChat, self.raw_data_path)
        with open(self.baseline_path + "group_batch.p", "rb") as f:
            baseline = pickle.load(f)
            self.assertEqual(group, baseline)
    
    def test_repeated_import(self):
        group = chatanalytics.ChatGroup()
        group.load(chatanalytics.DiscordChat, self.raw_data_path + self.direct_message_path)
        group.load(chatanalytics.DiscordChat, self.raw_data_path + self.group_message_path)
        group.load(chatanalytics.DiscordChat, self.raw_data_path + self.server_message_path)

    def test_batch_import_equals_import(self):
        groupA = chatanalytics.ChatGroup()
        groupA.batch_load(chatanalytics.DiscordChat, self.raw_data_path)

        groupB = chatanalytics.ChatGroup()
        groupB.load(chatanalytics.DiscordChat, self.raw_data_path + self.direct_message_path)
        groupB.load(chatanalytics.DiscordChat, self.raw_data_path + self.group_message_path)
        groupB.load(chatanalytics.DiscordChat, self.raw_data_path + self.server_message_path)

        self.assertEqual(groupA, groupB)
