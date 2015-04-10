# postbote

<img src="http://www.preiser-figuren-onlineshop.com/images/product_images/popup_images/Preiser/PREI045073.jpg" height="300px"/>

A tiny go program that takes messages from a Rabbit MQ queue and forwards them to an exchange of your choosing.

Messages are logged to stdout separated by new line.

This example would take all messages from `inbox.errorsQ` and forward them into the `inbox` exchange, while logging them into the `messages` file.

This works well for string based formats like json.
If you are sendig binary data, be aware that the output separates messages with a `\n` character.

```
postbote -q=inbox.errorsQ -x=inbox > messages
```
