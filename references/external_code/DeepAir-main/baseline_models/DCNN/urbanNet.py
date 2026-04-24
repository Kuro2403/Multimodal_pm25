
import torch
import torch.nn as nn
import torchvision.models as models


# Convolutional neural network (two convolutional layers)
class BasemapNet(nn.Module):
    def __init__(self, out_dim=20):
        super(BasemapNet, self).__init__()

        model_conv = models.resnet18(pretrained=True)
        num_ftrs = model_conv.fc.in_features
        model_conv.fc = nn.Sequential(
            nn.Linear(num_ftrs, 64, ),
            nn.LeakyReLU(0.2, inplace=False),
            nn.Linear(64, out_dim)
        )
        
        self.ResModel = model_conv

    def forward(self, image):
        x = self.ResModel(image)
        return x



# Convolutional neural network (two convolutional layers)
class urbanNet(nn.Module):
    def __init__(self):
        super(urbanNet, self).__init__()

        model_conv = models.resnet101(pretrained=False)
        num_ftrs = model_conv.fc.in_features
        model_conv.fc = nn.Sequential(
            nn.Linear(num_ftrs, 64, ),
            nn.Linear(64, 1)
        )
        
        self.ResModel = model_conv

    def forward(self, sample_batched):
        image = sample_batched["image"]
        if torch.cuda.is_available():
            image = image.cuda()
        entire_out = self.ResModel(image)
        return entire_out

    def eval_on_batch(self, sample_batched):
        entire_out = self(sample_batched)
        
        label = sample_batched['label']  # (16, 7)
        label = label.squeeze(1)
        if torch.cuda.is_available():
            label = label.cuda()


        loss_ = torch.nn.MSELoss() 
        entire_loss = loss_(label, entire_out)

        pred_dict = {'label': label, 'pred': entire_out}

        return pred_dict, entire_loss