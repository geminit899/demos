from tensorboard import program
import sys

if __name__ == '__main__':
    argv = [
        "",
        "--logdir", sys.argv[1],
        '--port', sys.argv[2],
    ]
    tensorboard = program.TensorBoard()
    tensorboard.configure(argv)
    print(tensorboard.main())
