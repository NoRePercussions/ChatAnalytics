import unittest
import pickle
import chatanalytics  # to be run in base directory


class DiscordChatTest(unittest.TestCase):
    baseline_path = "test/test_data/discord/baseline/"
    raw_data_path = "test/test_data/discord/messages/"
    direct_message_path = "c533895984269587"
    group_message_path = "c5662031163313723"
    server_message_path = "c5519593417670182"
    timezone = "Asia/Tokyo"

    def test_direct_message_import(self):
        group = chatanalytics.GenericChat()
        group.set_timezone(self.timezone)
        group.load(self.raw_data_path + self.direct_message_path)
        with open(self.baseline_path + self.direct_message_path + ".p", "rb") as f:
            baseline = pickle.load(f)
            baseline.set_timezone(self.timezone)
            self.assertEqual(group, baseline)

    def test_group_message_import(self):
        group = chatanalytics.GenericChat()
        group.set_timezone(self.timezone)
        group.load(self.raw_data_path + self.group_message_path)
        with open(self.baseline_path + self.group_message_path + ".p", "rb") as f:
            baseline = pickle.load(f)
            baseline.set_timezone(self.timezone)
            self.assertEqual(group, baseline)

    def test_server_message_import(self):
        group = chatanalytics.GenericChat()
        group.set_timezone(self.timezone)
        group.load(self.raw_data_path + self.server_message_path)
        with open(self.baseline_path + self.server_message_path + ".p", "rb") as f:
            baseline = pickle.load(f)
            baseline.set_timezone(self.timezone)
            self.assertEqual(group, baseline)

    def test_batch_import(self):
        group = chatanalytics.GenericChat()
        group.batch_load(self.raw_data_path)
        group.set_timezone(self.timezone)
        with open(self.baseline_path + "chat_batch.p", "rb") as f:
            baseline = pickle.load(f)
            baseline.set_timezone(self.timezone)
            self.assertEqual(group, baseline)

    def test_repeated_import(self):
        chat = chatanalytics.GenericChat()
        chat.set_timezone(self.timezone)
        chat.load(self.raw_data_path + self.direct_message_path)
        chat.load(self.raw_data_path + self.group_message_path)
        chat.load(self.raw_data_path + self.server_message_path)

    def test_batch_import_equals_import(self):
        chatA = chatanalytics.GenericChat()
        chatA.batch_load(self.raw_data_path)

        chatB = chatanalytics.GenericChat()
        chatB.load(self.raw_data_path + self.direct_message_path)
        chatB.load(self.raw_data_path + self.group_message_path)
        chatB.load(self.raw_data_path + self.server_message_path)

        self.assertEqual(chatA, chatB)



class MessengerChatTest(unittest.TestCase):
    baseline_path = "test/test_data/messenger/baseline/"
    raw_data_path = "test/test_data/messenger/messages/inbox/"
    end = "/message_1.json"
    direct_message_path = "directmessage_78o3u1q7"
    group_message_path = "groupmessage_99hdkg23"
    timezone = "Asia/Tokyo"

    def test_direct_message_import(self):
        group = chatanalytics.GenericChat()
        group.set_timezone(self.timezone)
        group.load(self.raw_data_path + self.direct_message_path + self.end)
        with open(self.baseline_path + self.direct_message_path + ".p", "rb") as f:
            baseline = pickle.load(f)
            baseline.set_timezone(self.timezone)
            self.assertEqual(group, baseline)

    def test_group_message_import(self):
        group = chatanalytics.GenericChat()
        group.set_timezone(self.timezone)
        group.load(self.raw_data_path + self.group_message_path + self.end)
        with open(self.baseline_path + self.group_message_path + ".p", "rb") as f:
            baseline = pickle.load(f)
            baseline.set_timezone(self.timezone)
            self.assertEqual(group, baseline)

    def test_batch_import(self):
        group = chatanalytics.GenericChat()
        group.set_timezone(self.timezone)
        group.batch_load(self.raw_data_path)
        with open(self.baseline_path + "chat_batch.p", "rb") as f:
            baseline = pickle.load(f)
            baseline.set_timezone(self.timezone)
            self.assertEqual(group, baseline)

    def test_repeated_import(self):
        chat = chatanalytics.GenericChat()
        chat.set_timezone(self.timezone)
        chat.load(self.raw_data_path + self.direct_message_path + self.end)
        chat.load(self.raw_data_path + self.group_message_path + self.end)

    def test_batch_import_equals_import(self):
        chatA = chatanalytics.GenericChat()
        chatA.batch_load(self.raw_data_path)

        chatB = chatanalytics.GenericChat()
        chatB.load(self.raw_data_path + self.direct_message_path + self.end)
        chatB.load(self.raw_data_path + self.group_message_path + self.end)

        self.assertEqual(chatA, chatB)
