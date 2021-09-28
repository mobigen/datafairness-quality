"""BERT NER Inference."""

from __future__ import absolute_import, division, print_function

import json
import os

import torch
import torch.nn.functional as F
from nltk import word_tokenize
from pytorch_transformers import (BertForTokenClassification,
                                  BertTokenizer)


class BertNer(BertForTokenClassification):

    def forward(self, input_ids, token_type_ids=None, attention_mask=None, valid_ids=None):
        sequence_output = self.bert(input_ids, token_type_ids, attention_mask, head_mask=None)[0]
        batch_size,max_len,feat_dim = sequence_output.shape
        valid_output = torch.zeros(batch_size,max_len,feat_dim,dtype=torch.float32,device='cuda' if torch.cuda.is_available() else 'cpu')
        for i in range(batch_size):
            jj = -1
            for j in range(max_len):
                    if valid_ids[i][j].item() == 1:
                        jj += 1
                        valid_output[i][jj] = sequence_output[i][j]
        sequence_output = self.dropout(valid_output)
        logits = self.classifier(sequence_output)
        return logits

class Ner:

    def __init__(self,model_dir: str):
        self.model , self.tokenizer, self.model_config = self.load_model(model_dir)
        self.label_map = self.model_config["label_map"]
        self.max_seq_length = self.model_config["max_seq_length"]
        self.label_map = {int(k):v for k,v in self.label_map.items()}
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = self.model.to(self.device)
        self.model.eval()

    def load_model(self, model_dir: str, model_config: str = "model_config.json"):
        model_config = os.path.join(model_dir,model_config)
        model_config = json.load(open(model_config))
        model = BertNer.from_pretrained(model_dir)
        tokenizer = BertTokenizer.from_pretrained(model_dir, do_lower_case=model_config["do_lower"])
        return model, tokenizer, model_config

    def tokenize(self, text: str):
        """ tokenize input"""
        words = word_tokenize(text)
        tokens_list = []
        tokens = []
        valid_positions_list = []
        valid_positions = []
        for index, word in enumerate(words):
            token = self.tokenizer.tokenize(word)
            tokens.extend(token)
            for i in range(len(token)):
                if i == 0:
                    valid_positions.append(1)
                else:
                    valid_positions.append(0)
            if len(tokens) >= 400:
                tokens_list.append(tokens)
                valid_positions_list.append(valid_positions)
                valid_positions = []
                tokens = []
            if (len(words) - 1) == index:
                tokens_list.append(tokens)
                valid_positions_list.append(valid_positions)

        return tokens_list, valid_positions_list

    def preprocess(self, text: str):
        """ preprocess """
        tokens_list, valid_positions_list = self.tokenize(text)
        input_ids_list = []
        input_mask_list = []
        segment_ids_list = []
        return_valid_positions_list = []
        for i in range(len(tokens_list)):
            ## insert "[CLS]"
            tokens = tokens_list[i]
            valid_positions = valid_positions_list[i]

            tokens.insert(0,"[CLS]")
            valid_positions.insert(0,1)
            ## insert "[SEP]"
            tokens.append("[SEP]")
            valid_positions.append(1)
            segment_ids = []
            for i in range(len(tokens)):
                segment_ids.append(0)

            input_ids = self.tokenizer.convert_tokens_to_ids(tokens)
            input_mask = [1] * len(input_ids)
            while len(input_ids) < self.max_seq_length:
                input_ids.append(0)
                input_mask.append(0)
                segment_ids.append(0)
                valid_positions.append(0)
            input_ids_list.append(input_ids)
            input_mask_list.append(input_mask)
            segment_ids_list.append(segment_ids)
            return_valid_positions_list.append(valid_positions)
        return input_ids_list, input_mask_list, segment_ids_list, return_valid_positions_list

    def predict(self, text: str):
        input_ids_list, input_mask_list ,segment_ids_list, valid_ids_list = self.preprocess(text)

        labels = []
        for i in range(len(input_ids_list)):
            input_ids = input_ids_list[i]
            input_mask = input_mask_list[i]
            segment_ids = segment_ids_list[i]
            valid_ids = valid_ids_list[i]

            input_ids = torch.tensor([input_ids],dtype=torch.long,device=self.device)
            input_mask = torch.tensor([input_mask],dtype=torch.long,device=self.device)
            segment_ids = torch.tensor([segment_ids],dtype=torch.long,device=self.device)
            valid_ids = torch.tensor([valid_ids],dtype=torch.long,device=self.device)
            with torch.no_grad():
                logits = self.model(input_ids, segment_ids, input_mask,valid_ids)
            logits = F.softmax(logits,dim=2)
            logits_label = torch.argmax(logits,dim=2)
            logits_label = logits_label.detach().cpu().numpy().tolist()[0]

            logits_confidence = [values[label].item() for values,label in zip(logits[0],logits_label)]

            logits = []
            pos = 0
            for index,mask in enumerate(valid_ids[0]):
                if index == 0:
                    continue
                if mask == 1:
                    logits.append((logits_label[index-pos],logits_confidence[index-pos]))
                else:
                    pos += 1
            logits.pop()
            for label,confidence in logits:
                labels.append((self.label_map[label],confidence))
        words = word_tokenize(text)
        assert len(labels) == len(words)
        output = [{"word":word,"tag":label,"confidence":confidence} for word,(label,confidence) in zip(words,labels)]
        return output