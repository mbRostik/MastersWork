pragma circom 2.2.2;

include "node_modules/circomlib/circuits/comparators.circom";
include "node_modules/circomlib/circuits/poseidon.circom";

template Reputation() {
    signal input postCommitment;
    signal input author;
    signal input postId;
    signal input textDigest;
    signal input saltHigh;
    signal input saltLow;
    signal input sentiment;
    signal input toxicity;
    signal input quality;
    signal output scoreCommitment;

    component subject = Poseidon(3);
    subject.inputs[0] <== author;
    subject.inputs[1] <== postId;
    subject.inputs[2] <== textDigest;

    component post = Poseidon(3);
    post.inputs[0] <== subject.out;
    post.inputs[1] <== saltHigh;
    post.inputs[2] <== saltLow;
    post.out === postCommitment;

    // Each metric is a probability in units of 1/10000. Range checks also
    // constrain the inputs used by the comparisons below.
    signal metrics[3];
    metrics[0] <== sentiment;
    metrics[1] <== toxicity;
    metrics[2] <== quality;
    component bits[3];
    component bounds[3];
    for (var i = 0; i < 3; i++) {
        bits[i] = Num2Bits(14);
        bits[i].in <== metrics[i];
        bounds[i] = LessEqThan(14);
        bounds[i].in[0] <== metrics[i];
        bounds[i].in[1] <== 10000;
        bounds[i].out === 1;
    }

    signal composite <== 30 * sentiment + 40 * (10000 - toxicity) + 30 * quality;
    component threshold = LessThan(20);
    threshold.in[0] <== 700000;
    threshold.in[1] <== composite;
    threshold.out === 1;

    component moderation = LessEqThan(14);
    moderation.in[0] <== toxicity;
    moderation.in[1] <== 5000;
    moderation.out === 1;

    component score = Poseidon(3);
    score.inputs[0] <== postCommitment;
    score.inputs[1] <== composite;
    score.inputs[2] <== saltHigh;
    scoreCommitment <== score.out;
}

component main {public [postCommitment]} = Reputation();
